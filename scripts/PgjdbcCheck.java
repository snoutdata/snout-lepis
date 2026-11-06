import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.postgresql.PGStatement;

/**
 * pgJDBC through the router in transaction mode, from `scripts/it.sh`: the real driver's server-side
 * prepared statements, on connections that do not keep a backend between transactions.
 *
 *   java -cp postgresql.jar PgjdbcCheck.java host port user password database [pgbouncer]
 *
 * With `pgbouncer`, through a PgBouncer in transaction mode, which keeps at most its pool size of
 * backends and refuses nothing.
 */
public class PgjdbcCheck {
	static String url;
	static Properties props = new Properties();

	static void check(boolean ok, String what) {
		if (!ok) {
			throw new AssertionError(what);
		}
		System.out.println("ok    pgjdbc: " + what);
	}

	static Connection open(int threshold) throws SQLException {
		Properties p = new Properties();
		p.putAll(props);
		p.setProperty("prepareThreshold", Integer.toString(threshold));
		return DriverManager.getConnection(url, p);
	}

	static String one(Connection c, String sql) throws SQLException {
		try (Statement s = c.createStatement(); ResultSet r = s.executeQuery(sql)) {
			r.next();
			return r.getString(1);
		}
	}

	public static void main(String[] a) throws Exception {
		url = "jdbc:postgresql://" + a[0] + ":" + a[1] + "/" + a[4] + "?sslmode=disable";
		props.setProperty("user", a[2]);
		props.setProperty("password", a[3]);

		boolean pgbouncer = a.length > 5 && a[5].equals("pgbouncer");
		try (Connection x = open(1); Connection y = open(1); Connection z = open(5)) {
			try (Statement s = x.createStatement()) {
				s.execute("delete from pgjdbc_check");
			}

			// The same statements, server-side prepared from the first use, turn about on two
			// connections and so on whichever backend each lands on.
			int id = 0;
			Connection[] both = { x, y };
			PreparedStatement[] writes = new PreparedStatement[2];
			PreparedStatement[] reads = new PreparedStatement[2];
			for (int i = 0; i < 2; i++) {
				writes[i] = both[i].prepareStatement("insert into pgjdbc_check values (?, ?)");
				reads[i] = both[i].prepareStatement("select v from pgjdbc_check where id = ?");
			}
			for (int round = 0; round < 40; round++) {
				int i = round % 2;
				id++;
				writes[i].setInt(1, id);
				writes[i].setString(2, "v" + id);
				check0(writes[i].executeUpdate() == 1, "an insert wrote its row");
				reads[i].setInt(1, id);
				try (ResultSet r = reads[i].executeQuery()) {
					check0(r.next() && r.getString(1).equals("v" + id), "a read saw its row");
				}
			}
			check(writes[0].unwrap(PGStatement.class).isUseServerPrepare(), "statements are server-side prepared");
			check(true, "40 prepared statements, turn about on two connections");

			// The driver's default: it starts preparing on the server at the fifth use.
			try (PreparedStatement p = z.prepareStatement("select count(*) from pgjdbc_check where id <= ?")) {
				for (int round = 0; round < 12; round++) {
					p.setInt(1, 40);
					try (ResultSet r = p.executeQuery()) {
						check0(r.next() && r.getInt(1) == 40, "the count stayed right");
					}
				}
				check(p.unwrap(PGStatement.class).isUseServerPrepare(), "the default threshold moves to server-side");
			}

			// What pooling saves: a login opens a backend, and the router keeps at most 64 of them idle
			// (`MAX_IDLE`), so 100 idle clients hold about 64 and not 100.
			java.util.List<Connection> many = new java.util.ArrayList<>();
			try {
				for (int n = 0; n < 97; n++) {
					Connection c = open(5);
					many.add(c);
					one(c, "select 1");
				}
				Thread.sleep(1000);
				int held = Integer.parseInt(one(x, "select count(*) from pg_stat_activity where usename = current_user"));
				System.out.println("note  pgjdbc: 100 clients open, " + held + " backends at the home");
				check(held <= (pgbouncer ? 20 : 66), pgbouncer ? "100 idle clients hold at most the pool's 20 backends" : "100 idle clients hold about 64 backends");
			} finally {
				for (Connection c : many) {
					c.close();
				}
			}

			// A transaction keeps its backend, and a rollback is a rollback.
			x.setAutoCommit(false);
			String first = one(x, "select pg_backend_pid()");
			id++;
			writes[0].setInt(1, id);
			writes[0].setString(2, "gone");
			writes[0].executeUpdate();
			one(y, "select pg_backend_pid()");
			check(first.equals(one(x, "select pg_backend_pid()")), "a transaction keeps its backend");
			x.rollback();
			reads[0].setInt(1, id);
			try (ResultSet r = reads[0].executeQuery()) {
				check(!r.next(), "a rollback took its row back");
			}
			x.commit();

			// A portal, read a few rows at a time: inside a transaction, as the driver requires.
			try (PreparedStatement p = x.prepareStatement("select id from pgjdbc_check order by id")) {
				p.setFetchSize(3);
				int seen = 0;
				try (ResultSet r = p.executeQuery()) {
					while (r.next()) {
						seen++;
					}
				}
				check(seen == 40, "a cursor read by fetch size saw every row");
			}
			x.commit();
			x.setAutoCommit(true);

			// A batch.
			try (PreparedStatement p = x.prepareStatement("insert into pgjdbc_check values (?, 'batch')")) {
				for (int n = 0; n < 5; n++) {
					p.setInt(1, 1000 + n);
					p.addBatch();
				}
				check(p.executeBatch().length == 5, "a batch ran");
			}
			check(one(y, "select count(*) from pgjdbc_check where v = 'batch'").equals("5"), "the batch is committed");

			// A session SET follows the client onto whichever backend it lands on.
			try (Statement s = x.createStatement()) {
				s.execute("set application_name = 'pgjdbc-check'");
			}
			one(y, "select 1");
			check(one(x, "show application_name").equals("pgjdbc-check"), "a session SET stays with its client");
			check(!"pgjdbc-check".equals(one(y, "show application_name")), "and does not reach another client");

			if (pgbouncer) {
				System.out.println("pgjdbc passed");
				return;
			}

			// What cannot be carried is refused, naming the setting, and the connection goes on.
			String code = null;
			String message = "";
			try (Statement s = x.createStatement()) {
				s.execute("listen somewhere");
			} catch (SQLException e) {
				code = e.getSQLState();
				message = e.getMessage();
			}
			check("0A000".equals(code) && message.contains("pool_mode"), "LISTEN is refused, naming the setting (got " + code + ": " + message + ")");
			check(one(x, "select 1").equals("1"), "the connection goes on after a refusal");

		}
		System.out.println("pgjdbc passed");
	}

	static void check0(boolean ok, String what) {
		if (!ok) {
			throw new AssertionError(what);
		}
	}
}

package native;

import static org.junit.Assert.*;

import java.sql.Timestamp;
import java.util.Arrays;

import org.joda.time.DateTime;
import org.junit.Test;

public class NativeSqlBuilderTest {

	@Test
	public void simple_where() {
		NativeSqlBuilder sql = NativeSqlBuilder.create();
		sql.select("field").from("tt").where(sql.eq("field", "value"));
		// System.out.println(sql.toString());
		String expected = "select field from tt where field = 'value'";
		assertEquals(expected, sql.toString());
	}
	
	@Test
	public void oracle_functions() {
		NativeSqlBuilder sql = NativeSqlBuilder.create();
		sql.select("field").from("tt").where(sql.eq("tt.field", sql.expr("substr(this_.locator, 1, instr(this_.locator, '[') - 1)")));
		// System.out.println(sql.toString());
		String expected = "select field from tt where tt.field = substr(this_.locator, 1, instr(this_.locator, '[') - 1)";
		assertEquals(expected, sql.toString());
	}
	
	@Test
	public void select_with_oracle_functions() {
		NativeSqlBuilder sql = NativeSqlBuilder.create();
		sql.select(sql.expr("initcap(field1)"), sql.expr("lower(field2)")).from("tt");
		// System.out.println(sql.toString());
		String expected = "select initcap(field1), lower(field2) from tt";
		assertEquals(expected, sql.toString());
	}
	
	@Test
	public void select_with_oracle_function_with_alias() {
		NativeSqlBuilder sql = NativeSqlBuilder.create();
		sql.select(sql.expr("initcap(field1)"), "alias").from("tt");
		// System.out.println(sql.toString());
		String expected = "select initcap(field1) alias from tt";
		assertEquals(expected, sql.toString());
	}
	
	@Test
	public void eq_column_name() {
		NativeSqlBuilder sql = NativeSqlBuilder.create();
		sql.select("field").from("tt").where(sql.eq("field", sql.expr("table2.anotherField")));
		//System.out.println(sql.toString());
		String expected = "select field from tt where field = table2.anotherField";
		assertEquals(expected, sql.toString());
	}
	
	@Test
	public void simple_where_intValue() {
		NativeSqlBuilder sql = NativeSqlBuilder.create();
		sql.select("field").from("tt").where(sql.eq("field", 123));
		// System.out.println(sql.toString());
		String expected = "select field from tt where field = 123";
		assertEquals(expected, sql.toString());
	}
	
	@Test
	public void like_escape() {
		NativeSqlBuilder sql = NativeSqlBuilder.create();
		sql.select("field").from("tt").where(sql.like("field", "%v!_alue%", '!'));
		// System.out.println(sql.toString());
		String expected = "select field from tt where field like '%v!_alue%' escape '!'";
		assertEquals(expected, sql.toString());
	}
	
	@Test
	public void like() {
		NativeSqlBuilder sql = NativeSqlBuilder.create();
		sql.select("field").from("tt").where(sql.like("field", "%value%"));
		// System.out.println(sql.toString());
		String expected = "select field from tt where field like '%value%' escape '\\'";
		assertEquals(expected, sql.toString());
	}
	
	@Test
	public void simple_where_with_tableName() {
		NativeSqlBuilder sql = NativeSqlBuilder.create();
		sql.select("tt.field", "anotherField").from("tt").where(sql.eq("tt.field", "value"));
		// System.out.println(sql.toString());
		String expected = "select tt.field, anotherField from tt where tt.field = 'value'";
		assertEquals(expected, sql.toString());
	}
	
	@Test
	public void count() {
		NativeSqlBuilder sql = NativeSqlBuilder.create();
		sql.select("field").countAll().from("tt").where(sql.eq("field", "value"));
		// System.out.println(sql.toString());
		String expected = "select count(*) from tt where field = 'value'";
		assertEquals(expected, sql.toString());
	}
	
	@Test
	public void countAllAs() {
		NativeSqlBuilder sql = NativeSqlBuilder.create();
		sql.select("field").countAllAs("countAlias").from("tt").where(sql.eq("field", "value"));
		// System.out.println(sql.toString());
		String expected = "select count(*) countAlias from tt where field = 'value'";
		assertEquals(expected, sql.toString());
	}
	
	@Test
	public void where_constant_eq_subQuery() {
		NativeSqlBuilder subQ = NativeSqlBuilder.create();
		subQ.select("a").from("tb");
		NativeSqlBuilder sql = NativeSqlBuilder.create();
		sql.select("field").from("tt").where(sql.eq(1, subQ));
		// System.out.println(sql.toString());
		String expected = "select field from tt where 1 = (select a from tb)";
		assertEquals(expected, sql.toString());
	}

	@Test
	public void predicatePlaceholder() {
		NativeSqlBuilder sql = NativeSqlBuilder.create();
		sql.select("field").from("tt").where(sql.predicatePlaceholder());
		// System.out.println(sql.toString());
		String expected = "select field from tt where 1 = 1";
		assertEquals(expected, sql.toString());
	}
	
	@Test
	public void exists() {
		NativeSqlBuilder sql = NativeSqlBuilder.create();
		NativeSqlBuilder subQ = NativeSqlBuilder.create();
		subQ.select("a").from("tb");
		sql.select("field").from("tt").where(sql.exists(subQ));
		// System.out.println(sql.toString());
		String expected = "select field from tt where exists (select a from tb)";
		assertEquals(expected, sql.toString());
	}

	@Test
	public void selectAll() {
		NativeSqlBuilder sql = NativeSqlBuilder.create();
		sql.selectAll().from("tt").where(sql.eq("field", "value"));
		// System.out.println(sql.toString());
		String expected = "select * from tt where field = 'value'";
		assertEquals(expected, sql.toString());
	}

	@Test
	public void left_join() {
		NativeSqlBuilder sql = NativeSqlBuilder.create();
		sql.select("field").from("tt").leftJoin("joinTable").on("left", "right")
				.where(sql.eq("field", "value"));
		// System.out.println(sql.toString());
		String expected = "select field from tt left join joinTable on left = right where field = 'value'";
		assertEquals(expected, sql.toString());
	}
	
	@Test
	public void left_join_alias() {
		NativeSqlBuilder sql = NativeSqlBuilder.create();
		sql.select("field").from("tt").leftJoin("joinTable", "jt").on("tt.left", "jt.right")
				.where(sql.eq("field", "value"));
		// System.out.println(sql.toString());
		String expected = "select field from tt left join joinTable jt on tt.left = jt.right where field = 'value'";
		assertEquals(expected, sql.toString());
	}
	
	@Test
	public void inner_join() {
		NativeSqlBuilder sql = NativeSqlBuilder.create();
		sql.select("field").from("tt").innerJoin("joinTable").on("left", "right")
				.where(sql.eq("field", "value"));
		// System.out.println(sql.toString());
		String expected = "select field from tt inner join joinTable on left = right where field = 'value'";
		assertEquals(expected, sql.toString());
	}
	
	@Test
	public void inner_join_with_aliases() {
		NativeSqlBuilder sql = NativeSqlBuilder.create();
		sql.select("field").from("tt", "alias1").innerJoin("joinTable", "alias2").on("alias1.left", "alias2.right")
				.where(sql.eq("field", "value"));
//		System.out.println(sql.toString());
		String expected = "select field from tt alias1 inner join joinTable alias2 on alias1.left = alias2.right where field = 'value'";
		assertEquals(expected, sql.toString());
	}
	
	@Test
	public void inner_join_with_tableNames() {
		NativeSqlBuilder sql = NativeSqlBuilder.create();
		sql.select("field").from("tt").innerJoin("joinTable").on("joinTable.left", "tt.right")
				.where(sql.eq("field", "value"));
		// System.out.println(sql.toString());
		String expected = "select field from tt inner join joinTable on joinTable.left = tt.right where field = 'value'";
		assertEquals(expected, sql.toString());
	}
	
	@Test
	public void multiple_joins_on_multiple_fields_with_tableNames() {
		NativeSqlBuilder sql = NativeSqlBuilder.create();
		sql.select("field")
			.from("tt")
			.innerJoin("joinTable")
				.on("joinTable.left", "tt.right")
				.on("joinTable.dbKey", "tt.anotherField")
			.leftJoin("secondJoin")
				.on("abc", "def")
				.on("secondJoin.fff", "tt.fff")
			.where(sql.eq("tt.field", "value"));
//		System.out.println(sql.toString());
		String expected = "select field from tt inner join joinTable on joinTable.left = tt.right and joinTable.dbKey = tt.anotherField "
							+ "left join secondJoin on abc = def and secondJoin.fff = tt.fff "
							+ "where tt.field = 'value'";
		assertEquals(expected, sql.toString());
	}
	
	@Test
	public void left_join_multiplefields() {
		NativeSqlBuilder sql = NativeSqlBuilder.create();
		sql.select("field").from("tt").leftJoin("joinTable").on("left", "right").on("aaa", "bbbb")
				.where(sql.eq("field", "value"));
		// System.out.println(sql.toString());
		String expected = "select field from tt left join joinTable on left = right and aaa = bbbb where field = 'value'";
		assertEquals(expected, sql.toString());
	}

	@Test
	public void implicit_and_with_explicit_or() {
		NativeSqlBuilder sql = NativeSqlBuilder.create();
		sql.select("field").from("tt").where(sql.eq("field", "value"), sql.or(sql.eq("f", "a"), sql.eq("g", "b")));
		// System.out.println(sql.toString());
		String expected = "select field from tt where field = 'value' and (f = 'a' or g = 'b')";
		assertEquals(expected, sql.toString());
	}

	@Test
	public void in_with_varargs() {
		NativeSqlBuilder sql = NativeSqlBuilder.create();
		sql.select("field").from("tt").where(sql.in("field", "a", "b"));
		// System.out.println(sql.toString());
		String expected = "select field from tt where field in ('a', 'b')";
		assertEquals(expected, sql.toString());
	}
	
	@Test
	public void in_with_list() {
		NativeSqlBuilder sql = NativeSqlBuilder.create();
		sql.select("field").from("tt").where(sql.in("field", Arrays.asList("a", "b")));
		// System.out.println(sql.toString());
		String expected = "select field from tt where field in ('a', 'b')";
		assertEquals(expected, sql.toString());
	}

	@Test
	public void subQuery() {
		NativeSqlBuilder b = NativeSqlBuilder.create();

		NativeSqlBuilder subQ = NativeSqlBuilder.create();
		subQ.select("a").from("tb");

		String expectedSub = "select a from tb";
		assertEquals(expectedSub, subQ.toString());
		
		b.select("a", "b").from("tt").where(b.in("a", subQ));

		String expected = "select a, b from tt where a in (select a from tb)";
		assertEquals(expected, b.toString());
	}

	@Test
	public void subQuery_listFields() {
		NativeSqlBuilder b = NativeSqlBuilder.create();

		NativeSqlBuilder subQ = NativeSqlBuilder.create();
		subQ.select("a").from("tb");
		
		String expectedSub = "select a from tb";
		assertEquals(expectedSub, subQ.toString());

		b.select("a", "b").from("tt").where(b.in(b.listFields("x", "y"), subQ));

		String expected = "select a, b from tt where (x, y) in (select a from tb)";
		assertEquals(expected, b.toString());
	}
	
	@Test
	public void eq_to_timestamp() {
		NativeSqlBuilder sql = NativeSqlBuilder.create();
		sql.select("field").from("tt").where(sql.eq("field", sql.to_timestamp(new DateTime(2018, 12, 31, 22, 45, 31))));
		// System.out.println(sql.toString());
		String expected = "select field from tt where field = to_timestamp('12/31/2018 22:45:31.0', 'mm/dd/yyyy hh24:mi:ss.ff3')";
		assertEquals(expected, sql.toString());
	}
	
	@Test
	public void lt_to_timestamp() {
		NativeSqlBuilder sql = NativeSqlBuilder.create();
		sql.select("field").from("tt").where(sql.lt("field", sql.to_timestamp(new DateTime(2018, 12, 31, 22, 45, 31))));
		// System.out.println(sql.toString());
		String expected = "select field from tt where field < to_timestamp('12/31/2018 22:45:31.0', 'mm/dd/yyyy hh24:mi:ss.ff3')";
		assertEquals(expected, sql.toString());
	}
	
	@Test
	public void gt_to_timestamp() {
		NativeSqlBuilder sql = NativeSqlBuilder.create();
		sql.select("field").from("tt").where(sql.gt("field", sql.to_timestamp(new DateTime(2018, 12, 31, 22, 45, 31))));
		// System.out.println(sql.toString());
		String expected = "select field from tt where field > to_timestamp('12/31/2018 22:45:31.0', 'mm/dd/yyyy hh24:mi:ss.ff3')";
		assertEquals(expected, sql.toString());
	}

	@Test
	public void escape() {
		String inputValue = "_aabc_dfg_jkl_";
		char escapeChar = '!';
		char charsToEscape = '_';
		String escaped = NativeSqlBuilder.escape(inputValue, escapeChar, charsToEscape);
		assertEquals("!_aabc!_dfg!_jkl!_", escaped);
	}
	
	@Test
	public void oracle_keywords() {
		NativeSqlBuilder sql = NativeSqlBuilder.create();
		sql.select("value", "id").from("tt").where(sql.eq("tt.VALUE", "value"), sql.eq("id", "true"));
//		System.out.println(sql.toString());
		String expected = "select \"value\", id from tt where tt.\"VALUE\" = 'value' and id = 'true'";
		assertEquals(expected, sql.toString());
	}
	
	@Test
	public void test_clone() {
		NativeSqlBuilder sql = NativeSqlBuilder.create();
		sql.select("field").from("tt").where(sql.eq("field", "value"));
//		System.out.println(sql.toString());
		String expected = "select field from tt where field = 'value'";
		assertEquals(expected, sql.toString());
		
		// clone 1 
		NativeSqlBuilder clone_1 = sql.clone();
		assertEquals(expected, clone_1.toString());
//		System.out.println(clone_1.toString());
		
		// clone 2 and modify 
		String expected_clone2 = "select field from tt where field = 'value' and fff = 'ggg'";
		NativeSqlBuilder clone_2 = sql.clone();
		clone_2.where(clone_2.eq("fff", "ggg"));
//		System.out.println(clone_2.toString());
		assertEquals(expected, clone_1.toString());
		assertEquals(expected_clone2, clone_2.toString());
		assertEquals(expected, sql.toString());
		
		// clone 3 and modify 
		String expected_clone3 = "select field from tt where field = 'value' and rrr = 'ttt'";
		NativeSqlBuilder clone_3 = sql.clone();
		clone_3.where(clone_3.eq("rrr", "ttt"));
//		System.out.println(clone_3.toString());
		assertEquals(expected, clone_1.toString());
		assertEquals(expected_clone2, clone_2.toString());
		assertEquals(expected_clone3, clone_3.toString());
		assertEquals(expected, sql.toString());

	}
	
	@Test
	public void convertTimeStampToJodaDateTime() {
		String theDateTime = "2017-11-20 20:45:23.123";
		Timestamp ts1 = Timestamp.valueOf(theDateTime);
		DateTime dt = NativeSqlBuilder.convertSQLTimeStampToJodaDateTime(ts1);
		String ts1_asString = ts1.toString();
		String dt_asString = dt.toString("yyyy-MM-dd HH:mm:ss.SSS");
		assertEquals(theDateTime, ts1_asString);
		assertEquals(theDateTime, dt_asString);
	}
	
	
}

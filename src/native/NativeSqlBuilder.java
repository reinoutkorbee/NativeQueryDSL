package native;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.joda.time.DateTime;

import com.querydsl.core.Tuple;
import com.querydsl.core.types.Expression;
import com.querydsl.core.types.Ops;
import com.querydsl.core.types.Predicate;
import com.querydsl.core.types.SubQueryExpression;
import com.querydsl.core.types.SubQueryExpressionImpl;
import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.core.types.dsl.BooleanOperation;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.core.types.dsl.NumberPath;
import com.querydsl.core.types.dsl.PathBuilder;
import com.querydsl.core.types.dsl.SimpleExpression;
import com.querydsl.core.types.dsl.StringExpression;
import com.querydsl.core.types.dsl.StringPath;
import com.querydsl.core.types.dsl.StringTemplate;
import com.querydsl.core.types.dsl.Wildcard;
import com.querydsl.sql.Configuration;
import com.querydsl.sql.OracleTemplates;
import com.querydsl.sql.SQLQuery;
import com.querydsl.sql.SQLTemplates;

public class NativeSqlBuilder {

	private final SQLQuery<?> q;
	
	private NativeSqlBuilder() {
		SQLTemplates tpl = new OracleTemplates(false); // Oracle doesn't recommend quoted identifiers
		Configuration config = new Configuration(tpl);
		config.setUseLiterals(true);
		SQLQuery<?> q = new SQLQuery<>(config);
		this.q = q;
	}
	
	private NativeSqlBuilder(SQLQuery<?> q) {
		this.q = q;
	}
	
	/**
	 * Clones the SQLQuery of this builder and returns a new builder with the same SQLQuery.
	 */
	@Override
	public NativeSqlBuilder clone() {
		SQLQuery<?> clonedQ = this.q.clone();
		NativeSqlBuilder clone = new NativeSqlBuilder(clonedQ);
		return clone;
	}
	
	/**
	 * 
	 * @return A NativeSqlBuilder instance
	 */
	public static NativeSqlBuilder create() {
		return new NativeSqlBuilder();
	}
	
	protected SQLQuery<?> getSQLQuery() {
		return q;
	}

	/**
	 * Returns the SQL as String.
	 */
	@Override
	public String toString() {
		return q.getSQL().getSQL().replace('\n', ' ');
	}
	
	/**
	 * 
	 *  <pre>left join joinTableName</pre>
	 * 
	 * @param joinTableName The table name to join
	 * @return NativeSqlBuilder
	 */
	public NativeSqlBuilder leftJoin(String joinTableName) {
		PathBuilder<String> join = new PathBuilder<>(String.class, joinTableName);
		q.leftJoin(join);
		return this;
	}
	
	/**
	 * 
	 *  <pre>left join joinTableName</pre>
	 * 
	 * @param joinTableName The table name to join
	 * @return NativeSqlBuilder
	 */
	public NativeSqlBuilder leftJoin(String joinTableName, String aliasName) {
		PathBuilder<String> join = new PathBuilder<>(String.class, joinTableName);
		StringPath alias = Expressions.stringPath(aliasName);
		q.leftJoin(join, alias);
		return this;
	}
	
	/**
	 * 
	 * <pre>on table1.leftFieldName = table2.rightFieldName </pre>
	 * 
	 * @param leftFieldName The left-hand side field name with optional table name, for example "tableName.fieldName"
	 * @param rightFieldName The right-hand side field name with optional table name, for example "tableName.fieldName"
	 * @return NativeSqlBuilder
	 */
	public NativeSqlBuilder on(String leftFieldName, String rightFieldName) {
		BooleanExpression joined = getPath(leftFieldName).eq(getPath(rightFieldName));
		q.on(joined);
		return this;
	}
	
	private NumberPath<Integer> getIntegerPath(String fieldName) {
		Optional<StringPath> parent = getParent(fieldName);
		String name = getFieldName(fieldName);
		NumberPath<Integer> path = parent.isPresent() ? Expressions.numberPath(Integer.class, parent.get(), name) : Expressions.numberPath(Integer.class, name);
		return path;
	}
	
	private NumberPath<Long> getLongPath(String fieldName) {
		Optional<StringPath> parent = getParent(fieldName);
		String name = getFieldName(fieldName);
		NumberPath<Long> path = parent.isPresent() ? Expressions.numberPath(Long.class, parent.get(), name) : Expressions.numberPath(Long.class, name);
		return path;
	}
	
	private StringPath getPath(String fieldName) {
		Optional<StringPath> parent = getParent(fieldName);
		String name = getFieldName(fieldName);
		StringPath path = parent.isPresent() ? Expressions.stringPath(parent.get(), name) : Expressions.stringPath(name);
		return path;
	}
	
	private Optional<StringPath> getParent(String fieldName) {
		if(fieldName.indexOf(".") > 0) {
			String[] paths = fieldName.split("\\.");
			if(paths.length == 2) {
				StringPath parent = Expressions.stringPath(paths[0]);
				return Optional.of(parent);
			}
		}
		return Optional.empty();
	}
	
	private String getFieldName(String fieldName) {
		if(fieldName.indexOf(".") > 0) {
			String[] paths = fieldName.split("\\.");
			if(paths.length == 2) {
				return paths[1];
			}
		}
		return fieldName;
	}
	
	/**
	 * 
	 * <pre>select field1, field2, table.field3</pre>
	 * 
	 * @param fieldNames A varargs arrays of field names with optional table names
	 * @return NativeSqlBuilder
	 */
	public NativeSqlBuilder select(String... fieldNames) {
		List<StringPath> fields = makeFields(Arrays.asList(fieldNames));
		q.select(fields.toArray(new StringPath[fields.size()]));
		return this;
	}
	
	/**
	 * Select a list of expressions.
	 * 
	 * <pre>select(sql.expr('initcap(columns1)'), sql.expr('lower(column2)'))</pre>
	 * 
	 * @param expressions The Expressions
	 * @return NativeSqlBuilder
	 */
	public NativeSqlBuilder select(StringExpression... expressions) {
		q.select(expressions);
		return this;
	}
	
	/**
	 * Select an expression and assign an alias.
	 * <pre>select(sql.expr('initcap(column)'), 'capitalized')</pre>
	 * 
	 * @param field The function
	 * @param aliasName The alias name
	 * @return NativeSqlBuilder
	 */
	public NativeSqlBuilder select(StringExpression expr, String aliasName) {
		q.select(expr.as(aliasName));
		return this;
	}
	
	private List<StringPath> makeFields(List<String> fieldNames) {
		List<StringPath> fields = fieldNames.stream().map(f -> getPath(f)).collect(Collectors.toList());
		return fields;
	}
	
	/**
	 * 
	 * <pre>select *</pre>
	 * 
	 * @return NativeSqlBuilder
	 */
	public NativeSqlBuilder selectAll() {
		q.select(Wildcard.all);
		return this;
	}
	
	/**
	 * 
	 * <pre>from tableName</pre>
	 * 
	 * @param tableName The table name
	 * @return NativeSqlBuilder
	 */
	public NativeSqlBuilder from(String tableName) {
		StringPath table = Expressions.stringPath(tableName);
		q.from(table);
		return this;
	}
	
	/**
	 * <pre>from(sql, 'anAlias')</pre>
	 * 
	 * @param subQueryBuilder The sub-query
	 * @param aliasName The alias to reference the results of the sub-query
	 * @return NativeSqlBuilder
	 */
	public NativeSqlBuilder from(NativeSqlBuilder subQueryBuilder, String aliasName) {
		SQLQuery<?> subQuery = subQueryBuilder.getSQLQuery();
		SubQueryExpression<String> subQueryExpression = new SubQueryExpressionImpl<String>(String.class,subQuery.getMetadata());
		StringPath alias = Expressions.stringPath(aliasName);
		q.from(subQueryExpression, alias);
		return this;
	}
	
	/**
	 * 
	 * <pre>from tableName alias</pre>
	 * 
	 * @param tableName The table name
	 * @param alias The table alias
	 * @return NativeSqlBuilder
	 */
	public NativeSqlBuilder from(String tableName, String aliasName) {
		StringPath table = Expressions.stringPath(tableName);
		q.from(table.as(aliasName));
		return this;
	}
	
	/**
	 * 
	 * <pre>where field = 'value'</pre>
	 * 
	 * @param predicates A varargs array of predicates for the where clause
	 * @return NativeSqlBuilder
	 */
	public NativeSqlBuilder where(Predicate... predicates) {
		q.where(predicates);
		return this;
	}
	
	/**
	 * 
	 * <pre>tableName.fieldName = 'value'</pre>
	 * 
	 * @param fieldName The field name with optional table name, for example "tableName.fieldName"
	 * @param value The String value to match
	 * @return BooleanExpression
	 */
	public BooleanExpression eq(String fieldName, String value) {
		return getPath(fieldName).eq(value);
	}
	
	/**
	 * 
	 * <pre>tableName.fieldName = 123</pre>
	 * 
	 * @param fieldName The field name with optional table name, for example "tableName.fieldName"
	 * @param value The Integer value to match
	 * @return BooleanExpression
	 */
	public BooleanExpression eq(String fieldName, Integer value) {
		return getIntegerPath(fieldName).eq(value);
	}
	
	/**
	 * 
	 * <pre>tableName.fieldName = 123456l</pre>
	 * 
	 * @param fieldName The field name with optional table name, for example "tableName.fieldName"
	 * @param value The Long value to match
	 * @return BooleanExpression
	 */
	public BooleanExpression eq(String fieldName, Long value) {
		return getLongPath(fieldName).eq(value);
	}
	
	/**
	 * Produces Oracle specific limit query.
	 * <pre>select * from (select * from tableName) where maxRows <= limit</pre>
	 * 
	 * @param limit The maximum number of rows to return
	 * @return NativeSqlBuilder
	 */
	public NativeSqlBuilder limit(long limit) {
		q.limit(limit);
		return this;
	}
	
	/**
	 * 
	 * <pre>fieldName in ('value1', 'value2', 'value3')</pre>
	 * 
	 * @param fieldName The field name with optional table name, for example "tableName.fieldName" 
	 * @param values A list with string values
	 * @return BooleanExpression
	 */
	public BooleanExpression in(String fieldName, String... values) {
		return getPath(fieldName).in(values);
	}
	
	/**
	 * 
	 * <pre>fieldName in ("v1", "v2", "v3")</pre>
	 * 
	 * @param fieldName The field name with optional table name, for example "tableName.fieldName" 
	 * @param values A list with string values
	 * @return BooleanExpression
	 */
	public BooleanExpression in(String fieldName, List<String> values) {
		return getPath(fieldName).in(values);
	}
	
	/**
	 * 
	 * <pre>fieldName in (select f1, f2 from temporary_table)</pre>
	 * 
	 * @param fieldName The field name with optional table name, for example "tableName.fieldName"  
	 * @param subQuery The sub-query to select from
	 * @return BooleanExpression
	 */
	public BooleanExpression in(String fieldName, NativeSqlBuilder subQueryBuilder) {
		SQLQuery<?> subQuery = subQueryBuilder.getSQLQuery();
		SubQueryExpression<String> subQueryExpression = new SubQueryExpressionImpl<String>(String.class,subQuery.getMetadata());
		return getPath(fieldName).in(subQueryExpression);
	}
	
	/**
	 * 
	 * <pre>(f1, f2, f3) in (select a, b, c from temporary_table)</pre>
	 * 
	 * @param listFields A list of fields to project the results of the sub-query into
	 * @param subQuery The sub-query with values to match 
	 * @return BooleanExpression
	 */
	public BooleanExpression in(SimpleExpression<Tuple> listFields, NativeSqlBuilder subQueryBuilder) {
		SQLQuery<?> subQuery = subQueryBuilder.getSQLQuery();
		SubQueryExpression<Tuple> subQueryExpression = new SubQueryExpressionImpl<Tuple>(Tuple.class,subQuery.getMetadata());
		return listFields.in(subQueryExpression);
	}
	
	/**
	 * 
	 * The result of the method is the input for {@link #in(SimpleExpression, SQLQuery)}
	 * 
	 * <pre>(fieldName1, fieldName2, fieldName3)</pre>
	 * 
	 * @param fieldNames The field names
	 * @return SimpleExpression
	 */
	public SimpleExpression<Tuple> listFields(String... fieldNames) {
		List<StringExpression> fields = Arrays.stream(fieldNames)
									.map(f -> getPath(f))
									.collect(Collectors.toList());
		return Expressions.list(fields.toArray(new StringPath[fields.size()]));
	}
	
	/**
	 * 
	 * <pre>field = 'value' and anOtherField > 2</pre>
	 * 
	 * @param expr The result of an eq(), lt() or gt() call
	 * @return BooleanExpression
	 */
	public BooleanExpression and(BooleanExpression... expr) {
		return Arrays.stream(expr).reduce((a, b) -> a.and(b)).get();
	}
	
	/**
	 * 
	 * <pre>field = 'value' or anOtherField > 2</pre>
	 * 
	 * @param expr The result of an eq(), lt() or gt() call
	 * @return BooleanExpression
	 */
	public BooleanExpression or(BooleanExpression... expr) {
		return Arrays.stream(expr).reduce((a, b) -> a.or(b)).get();
	}
	
	/**
	 * The Oracle specific predicate placeholder.
	 * 
	 * <pre>1 = 1</pre>
	 * 
	 * @return BooleanExpression
	 */
	public BooleanExpression predicatePlaceholder() {
		return Expressions.template(Integer.class, "1").eq(1);
	}
	
	/**
	 *
	 * <pre>3 = (select count(*) from temporary_table)</pre>
	 * 
	 * @param constant A number
	 * @param subQueryBuilder The sub-query
	 * @return BooleanExpression
	 */
	public BooleanExpression eq(Integer constant, NativeSqlBuilder subQueryBuilder) {
		SQLQuery<?> subQuery = subQueryBuilder.getSQLQuery();
		SubQueryExpression<Integer> subQueryExpression = new SubQueryExpressionImpl<Integer>(Integer.class,subQuery.getMetadata());
		return Expressions.template(Integer.class, String.valueOf(constant)).eq(subQueryExpression);
	}
	
	/**
	 * 
	 * <pre>exists (select field from table where field = 'value')</pre>
	 * 
	 * @param subQueryBuilder The sub-query which returns values to test for existence
	 * @return BooleanOperation
	 */
	public BooleanOperation exists(NativeSqlBuilder subQueryBuilder) {
		SQLQuery<?> subQuery = subQueryBuilder.getSQLQuery();
		SubQueryExpression<Tuple> subQueryExpression = new SubQueryExpressionImpl<Tuple>(Tuple.class,subQuery.getMetadata());
		return Expressions.booleanOperation(Ops.EXISTS, subQueryExpression);
	}
	
	/**
	 * <pre>count(*)</pre>
	 * 
	 * @return NativeSqlBuilder
	 */
	public NativeSqlBuilder countAll() {
		q.select(Wildcard.count);
		return this;
	}
	
	/**
	 * <pre>count(*) aliasName</pre>
	 * 
	 * @param aliasName The alias for the column with the count
	 * @return NativeSqlBuilder
	 */
	public NativeSqlBuilder countAllAs(String aliasName) {
		q.select(Wildcard.count.as(aliasName));
		return this;
	}
	
	/**
	 * 
	 * <pre>inner join joinTableName</pre>
	 * 
	 * @param joinTableName The table to join on
	 * @return NativeSqlBuilder
	 */
	public NativeSqlBuilder innerJoin(String joinTableName) {
		PathBuilder<String> join = new PathBuilder<>(String.class, joinTableName);
		q.innerJoin(join);
		return this;
	}
	
	/**
	 * 
	 * <pre>inner join joinTableName alias</pre>
	 * 
	 * @param joinTableName The table to join on
	 * @param alias The table alias
	 * @return NativeSqlBuilder
	 */
	public NativeSqlBuilder innerJoin(String joinTableName, String aliasName) {
		PathBuilder<String> join = new PathBuilder<>(String.class, joinTableName);
		StringPath alias = Expressions.stringPath(aliasName);
		q.innerJoin(join, alias);
		return this;
	}
	
	/**
	 * 
	 * <pre>field like '%abc%'</pre>
	 * 
	 * @param fieldName The field name with optional table name, for example "tableName.fieldName" 
	 * @param value The value with wild cards, for example "%abc%"
	 * @param escape A custom escape character, for example '!'
	 * @return BooleanExpression
	 */
	public BooleanExpression like(String fieldName, String value, char escape) {
		return getPath(fieldName).like(value, escape);
	}
	
	/**
	 * The '\' is used as default escape.
	 * 
	 * <pre>field like '%abc%'</pre>
	 * 
	 * @param fieldName The field name with optional table name, for example "tableName.fieldName" 
	 * @param value The value with wild cards, for example "%abc%"
	 * @return BooleanExpression
	 */
	public BooleanExpression like(String fieldName, String value) {
		return getPath(fieldName).like(value);
	}
	
	/**
	 * <pre>substr(field, 1, instr(locator, '[') - 1)</pre>
	 * 
	 * @param expr The Oracle specific function
	 * @return StringTemplate
	 */
	public StringTemplate expr(String expr) {
		return Expressions.stringTemplate(expr);
	}
	
	/**
	 * 
	 * <pre>fieldName = substr(field, 1, Instr(locator, '[') - 1)</pre>
	 * 
	 * @param fieldName The field name with optional table name, for example "tableName.fieldName" 
	 * @param expr An expression produced by, forn example: {@link #expr(String)} or {@link #to_timestamp(DateTime)}
	 * @return BooleanExpression
	 */
	public BooleanExpression eq(String fieldName, Expression<String> expr) {
		return getPath(fieldName).eq(expr);
	}
	
	/**
	 * 
	 * <pre>fieldName > to_timestamp('01/01/2011 10:12:34.000', 'mm/dd/yyyy hh24:mi:ss.ff3')</pre>
	 * 
	 * @param fieldName The field name with optional table name, for example "tableName.fieldName" 
	 * @param expr An expression produced by, forn example: {@link #expr(String)} or {@link #to_timestamp(DateTime)}
	 * @return BooleanExpression
	 */
	public BooleanExpression gt(String fieldName, Expression<String> expr) {
		return getPath(fieldName).gt(expr);
	}
	
	/**
	 * 
	 * <pre>fieldName > to_timestamp('01/01/2011 10:12:34.000', 'mm/dd/yyyy hh24:mi:ss.ff3')</pre>
	 * 
	 * @param fieldName The field name with optional table name, for example "tableName.fieldName" 
	 * @param expr An expression produced by, forn example: {@link #expr(String)} or {@link #to_timestamp(DateTime)}
	 * @return BooleanExpression
	 */
	public BooleanExpression lt(String fieldName, Expression<String> expr) {
		return getPath(fieldName).lt(expr);
	}
	
	/**
	 * Produces an expression as input for, for example: {@link #eq(String, Expression)}, {@link #gt(String, Expression)} or {@link #lt(String, Expression)}
	 * 
	 * <pre>to_timestamp('01/01/2011 10:12:34.000', 'mm/dd/yyyy hh24:mi:ss.ff3')</pre>
	 * 
	 * @param dateTime The date time to match
	 * @return StringTemplate
	 */
	public StringTemplate to_timestamp(DateTime dateTime) {
		String format = "mm/dd/yyyy hh24:mi:ss.ff3";
		String dtAsStr = dateTime.toString("MM/dd/yyyy HH:mm:ss.S");
		return Expressions.stringTemplate("to_timestamp({0}, {1})", dtAsStr, format);
	}
	
	/**
	 * Escape all occurrences of charsToEscape in the inputValue with the escape character.
	 * 
	 * <pre>escape("abc_dfg", '!', '_') = "abc!_dfg"</pre>
	 * 
	 * @param inputValue
	 * @param charsToEscape
	 * @param escapeChar 
	 * @return the escaped string
	 */
	public static String escape(String inputValue, char escapeChar, char... charsToEscape) {
		String escapedValue = inputValue;
		for(char charToEscape : charsToEscape){
			StringBuilder sb = new StringBuilder();
			String escaped = sb.append(escapeChar).append(charToEscape).toString();
			escapedValue = escapedValue.replaceAll(String.valueOf(charToEscape), escaped);
		}
		return escapedValue;
	}
	
	/**
	 * Convert a java.sql.Timestamp to an org.joda.time.DateTime object, using the default locale.
	 * @param ts The time stamp
	 * @return A DateTime
	 */
	public static DateTime convertSQLTimeStampToJodaDateTime(Timestamp ts) {
		LocalDateTime ldt = ts.toLocalDateTime();
		int millis = ldt.getNano() / 1_000_000;
		DateTime datetime = new DateTime(ldt.getYear(), ldt.getMonthValue(), ldt.getDayOfMonth(), ldt.getHour(), ldt.getMinute(), ldt.getSecond(), millis);
		return datetime;
	}
}


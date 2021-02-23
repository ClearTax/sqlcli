#![feature(box_syntax, box_patterns)]
#![allow(clippy::clippy::needless_return)]

extern crate sqlparser;

use sqlparser::ast::*;

use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::collections::HashSet;

pub fn parse_select_statement(sql: String) -> Result<Query, String> {
    let dialect = GenericDialect {};
    let results = Parser::parse_sql(&dialect, &sql);

    match results {
        Err(error) => {
            return Err(format!("Parsing error: {}", error.to_string()));
        }
        Ok(mut ast) => {
            if ast.is_empty() {
                return Err("Empty input".to_string());
            }

            if ast.len() > 1 {
                return Err("Expected only a single statement.".to_string());
            }

            let statement = ast.remove(0);

            match statement {
                Statement::Query(query) => Ok(*query),
                _ => Err("Expected a SELECT query.".to_string()),
            }
        }
    }
}

pub fn get_table_names(sql: String) -> Result<Vec<String>, String> {
    match parse_select_statement(sql) {
        Ok(query) => Ok(table_names_from_query(query)),
        Err(e) => Err(e),
    }
}

fn table_names_from_query(query: Query) -> Vec<String> {
    let mut table_names: HashSet<String> = HashSet::new();

    // Keep track of CTE names. Omit these CTEs from the final output.
    // Eg: `with (select * from employees) as employee_data select count(*) from employee_data`
    // Should return with `employees` (the actual table) and exclude `employee_data` (the internal CTE name).
    let mut cte_names: HashSet<String> = HashSet::new();

    if let Some(with) = query.with {
        for cte in with.cte_tables {
            // CTE names are lower-cased to make comparisons easier.
            cte_names.insert(cte.alias.to_string().to_lowercase());
            table_names.extend(table_names_from_query(cte.query));
        }
    }

    let from_body = table_names_from_set_expr(query.body);
    table_names.extend(from_body);

    let mut output: Vec<String> = Vec::new();

    let difference = table_names.difference(&cte_names);
    for name in difference {
        // Double-check lower-case version of table name, skip any that match.
        if cte_names.contains(&name.to_lowercase()) {
            continue;
        };

        output.push(name.to_string());
    }

    output.sort();
    return output;
}

fn table_names_from_set_expr(body: SetExpr) -> Vec<String> {
    match body {
        SetExpr::Select(select) => {
            let mut table_names: Vec<String> = Vec::new();

            for select_item in select.projection {
                match select_item {
                    SelectItem::UnnamedExpr(expr) => {
                        table_names.extend(table_names_from_expr(expr));
                    }
                    SelectItem::ExprWithAlias { expr, .. } => {
                        table_names.extend(table_names_from_expr(expr));
                    }
                    _ => {}
                }
            }

            for from in select.from {
                let from_name = table_names_from_table_factor(from.relation);
                table_names.extend(from_name);

                for join in from.joins {
                    let join_name = table_names_from_table_factor(join.relation);
                    table_names.extend(join_name);
                }
            }

            // TODO: select.lateral_views (Hive specific)

            if let Some(e) = select.selection {
                table_names.extend(table_names_from_expr(e));
            }

            if let Some(e) = select.having {
                table_names.extend(table_names_from_expr(e));
            }

            for exprs in vec![
                select.group_by,
                select.cluster_by,
                select.distribute_by,
                select.sort_by,
            ] {
                table_names.extend(table_names_from_exprs(exprs));
            }

            return table_names;
        }

        SetExpr::SetOperation {
            box left,
            box right,
            ..
        } => {
            let mut table_names = table_names_from_set_expr(left);
            let l2 = table_names_from_set_expr(right);
            table_names.extend(l2);
            return table_names;
        }

        SetExpr::Query(query) => {
            return table_names_from_query(*query);
        }

        SetExpr::Values(_) => {
            return vec![];
        }
        SetExpr::Insert(_) => {
            return vec![];
        }
    }
}

fn table_names_from_table_factor(t: TableFactor) -> Vec<String> {
    match t {
        TableFactor::Table { name, args, .. } => {
            // Skip over Tables with 'unnest' (expanding array or map into relation)
            if !args.is_empty() && name.to_string().eq_ignore_ascii_case("unnest") {
                return Vec::new();
            }
            return vec![name.to_string()];
        }
        TableFactor::Derived { subquery, .. } => {
            return table_names_from_query(*subquery);
        }

        TableFactor::TableFunction { expr, .. } => {
            return table_names_from_expr(expr);
        }

        TableFactor::NestedJoin(table_with_joins) => {
            let mut result = vec![];
            result.extend(table_names_from_table_factor(table_with_joins.relation));

            for join in table_with_joins.joins {
                result.extend(table_names_from_table_factor(join.relation));
            }
            return result;
        }
    }
}

fn table_names_from_exprs(exprs: Vec<Expr>) -> Vec<String> {
    let mut results = vec![];
    for expr in exprs {
        results.extend(table_names_from_expr(expr));
    }
    return results;
}

fn table_names_from_expr(expr: Expr) -> Vec<String> {
    match expr {
        Expr::IsNull(inner) => {
            return table_names_from_expr(*inner);
        }
        Expr::IsNotNull(inner) => {
            return table_names_from_expr(*inner);
        }
        Expr::InList { expr, list, .. } => {
            let mut results = table_names_from_exprs(list);
            results.extend(table_names_from_expr(*expr));
            return results;
        }
        Expr::InSubquery { expr, subquery, .. } => {
            let mut results = table_names_from_query(*subquery);
            results.extend(table_names_from_expr(*expr));
            return results;
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            let mut results = table_names_from_expr(*expr);
            results.extend(table_names_from_expr(*low));
            results.extend(table_names_from_expr(*high));
            return results;
        }
        Expr::BinaryOp { left, right, .. } => {
            let mut results = table_names_from_expr(*left);
            results.extend(table_names_from_expr(*right));
            return results;
        }
        Expr::UnaryOp { expr, .. } => {
            return table_names_from_expr(*expr);
        }
        Expr::Cast { expr, .. } => {
            return table_names_from_expr(*expr);
        }
        Expr::Extract { expr, .. } => {
            return table_names_from_expr(*expr);
        }
        Expr::Substring { expr, .. } => {
            return table_names_from_expr(*expr);
        }
        Expr::Collate { expr, .. } => {
            return table_names_from_expr(*expr);
        }
        Expr::Nested(expr) => {
            return table_names_from_expr(*expr);
        }
        Expr::MapAccess { column, .. } => {
            return table_names_from_expr(*column);
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            let mut vec = vec![];
            vec.extend(table_names_from_exprs(conditions));
            vec.extend(table_names_from_exprs(results));

            if let Some(e) = operand {
                vec.extend(table_names_from_expr(*e));
            }

            if let Some(e) = else_result {
                vec.extend(table_names_from_expr(*e));
            }

            return vec;
        }
        Expr::Exists(query) => {
            return table_names_from_query(*query);
        }
        Expr::Subquery(query) => {
            return table_names_from_query(*query);
        }

        Expr::Function(_) => {} // TODO

        Expr::ListAgg(list_agg) => {
            let mut vec = vec![];

            vec.extend(table_names_from_expr(*list_agg.expr));

            if let Some(e) = list_agg.separator {
                vec.extend(table_names_from_expr(*e));
            }

            // TODO on_overflow, within_group

            return vec;
        }

        Expr::Value(_) => {}
        Expr::TypedString { .. } => {}
        Expr::Identifier(_) => {}
        Expr::Wildcard => {}
        Expr::QualifiedWildcard(_) => {}
        Expr::CompoundIdentifier(_) => {}
    }

    return vec![];
}

#[cfg(test)]
mod table_name_tests {

    fn assert(input: &str, table_names: Vec<&str>) {
        let parsed_names_result = super::get_table_names(input.to_string());
        assert_eq!(true, parsed_names_result.is_ok());

        let parsed_names = parsed_names_result.unwrap();

        assert_eq!(table_names, parsed_names);
    }

    #[test]
    fn simple() {
        assert("select * from example", vec!["example"]);
    }

    #[test]
    fn nested_table_name() {
        assert("select * from (example)", vec!["example"]);
    }

    #[test]
    fn single_join() {
        assert(
            "select * from example1 join example2 on example1.id = example2.id",
            vec!["example1", "example2"],
        );
    }

    #[test]
    fn multi_join() {
        assert(
            "select * from example1 join example2 on example1.id = example2.id join example3 on id2 = example3.id2",
            vec!["example1", "example2", "example3"]
        );
    }

    #[test]
    fn simple_cte() {
        assert(
            "with a as (select * from example) select count(*) from a",
            vec!["example"],
        )
    }

    #[test]
    fn multiple_ctes() {
        assert(
            "with a as (select * from example_1),
            b as (select * from example_2)
            select
                (select count(*) from a) a_count,
                (select count(*)  from b) b_count",
            vec!["example_1", "example_2"],
        )
    }

    #[test]
    fn cte_with_different_case() {
        assert(
            "with abc  as (select * from table_1), xyz as (select * from table_2) select * from abc join Xyz on abc.id = xyz.id",
            vec!["table_1", "table_2"]
        );
    }

    #[test]
    fn naked_from() {
        assert(
            "
            select
                (select * from example_1) one,
                (select * from example_2) two
        ",
            vec!["example_1", "example_2"],
        )
    }

    #[test]
    fn inner_query() {
        assert(
            "
            select count(*) from (
                select * from example
            )
        ",
            vec!["example"],
        )
    }

    #[test]
    fn union() {
        assert(
            "
            select * from example_1
            union
            select * from example_2
        ",
            vec!["example_1", "example_2"],
        );
    }

    #[test]
    fn date() {
        assert("Select cast(t as date) as x from example", vec!["example"]);
    }

    #[test]
    fn date_fn() {
        assert("select date(created_at) from example", vec!["example"]);
    }

    #[test]
    fn cross_product() {
        // Query multiple tables without a join clause.
        assert(
            "select a.*, b.* from table_1 a, table_2 b",
            vec!["table_1", "table_2"],
        );
    }
    #[test]
    fn unnest() {
        assert(
            "SELECT student, score FROM tests CROSS JOIN UNNEST(scores) AS t (score);",
            vec!["tests"],
        );
    }

    #[test]
    fn in_query() {
        assert(
            "select * from video_views where user_id in (select id from users where plan = 'Ultra')",
            vec!["users", "video_views"]
        )
    }

    #[test]
    fn no_table() {
        assert("select 'hello world'", vec![]);
    }

    #[test]
    fn case_with_sub_query() {
        assert(
    "select 
            abc, 
            (case when user_id > 0 then (select max(active_time) from users where id = user_id) else NULL end) last_active_time
          from events",
vec!["events", "users"]);
    }

    #[test]
    fn window_function() {
        assert(
            "select emp_name, dealer_id, sales, avg(sales) over (partition by dealer_id) as avgsales from q1_sales;", 
            vec!["q1_sales"]);
    }
}

pub fn get_projection_names(sql: String) -> Result<Vec<String>, String> {
    match parse_select_statement(sql) {
        Ok(query) => {
            return Ok(projection_names_from_query(query));
        }

        Err(e) => {
            return Err(e);
        }
    }
}

fn projection_names_from_query(query: Query) -> Vec<String> {
    return projection_names_from_set_expr(query.body);
}

fn projection_names_from_set_expr(set_expr: SetExpr) -> Vec<String> {
    match set_expr {
        SetExpr::Select(select) => {
            return projection_names_from_projection(select.projection);
        }
        SetExpr::Query(query) => {
            return projection_names_from_query(*query);
        }
        SetExpr::SetOperation { left, .. } => {
            // Assuming a valid query,
            // both sides of an set operation (eg: UNION)
            // should have the same number of columns.

            // May not be possible to statically assert since something like
            // `SELECT a, b from t1 UNION select * from t2`
            // Would have a wildcard on the RHS.
            return projection_names_from_set_expr(*left);
        }
        _ => {
            return vec![];
        }
    }
}

fn projection_names_from_projection(projection: Vec<SelectItem>) -> Vec<String> {
    let mut result: Vec<String> = vec![];

    for item in projection {
        match item {
            SelectItem::UnnamedExpr(e) => {
                result.push(projection_name_from_expr(e));
            }
            SelectItem::ExprWithAlias { alias, .. } => {
                result.push(format!("NAMED: {}", alias.value));
            }
            SelectItem::QualifiedWildcard(qualified_wildcard) => {
                result.push(format!("WILDCARD: {}.*", qualified_wildcard));
            }
            SelectItem::Wildcard => {
                result.push("WILDCARD: *".to_string());
            }
        }
    }

    return result;
}

fn projection_name_from_expr(e: Expr) -> String {
    match e {
        Expr::Identifier(ident) => {
            // Simple case: Select column_name from table;
            return format!("NAMED: {}", ident.value);
        }

        Expr::CompoundIdentifier(mut compound) => {
            // For an expression: table_name.column_name
            // Return the last part: column_name
            // Vec should never be empty.
            return format!("NAMED: {}", compound.pop().unwrap().value);
        }

        Expr::Nested(e) => {
            return projection_name_from_expr(*e);
        }

        _ => {
            // IN all other circumstances, we don't know the column name.
            return "UNNAMED".to_string();
        }
    }
}

#[cfg(test)]
mod projection_tests {

    fn assert(sql: &str, args: Vec<&str>) {
        let parsed_projection = super::get_projection_names(sql.to_string());
        assert_eq!(true, parsed_projection.is_ok());

        let parsed_names = parsed_projection.unwrap();

        assert_eq!(args, parsed_names);
    }

    #[test]
    fn simple_test_wildcard() {
        assert("Select * from test", vec!["WILDCARD: *"]);
        assert("Select t.* from test t", vec!["WILDCARD: t.*"]);
    }

    #[test]
    fn simple_test_column_names_expr() {
        assert("Select c1, c2 from test", vec!["NAMED: c1", "NAMED: c2"]);
        assert("Select c1, c1 from test", vec!["NAMED: c1", "NAMED: c1"]);
    }

    #[test]
    fn simple_test_alias() {
        assert(
            "Select first_col c1, second_col c2 from test",
            vec!["NAMED: c1", "NAMED: c2"],
        );
    }

    #[test]
    fn simple_test_unnamed_expr() {
        assert("Select count(*) from test t", vec!["UNNAMED"]);
        assert(
            "Select max(id), min(id) from test t",
            vec!["UNNAMED", "UNNAMED"],
        );
    }

    #[test]
    fn union_test() {
        assert(
            "select a, b from t1 union select a, b from t2",
            vec!["NAMED: a", "NAMED: b"],
        );
    }

    #[test]
    fn distinct_test() {
        assert("select distinct name from users", vec!["NAMED: name"]);
        assert("select distinct(name) from users", vec!["NAMED: name"]);
    }

    #[test]
    fn cte_test() {
        assert(
            "with inner as (select * from table_1) select col1, col2 from inner",
            vec!["NAMED: col1", "NAMED: col2"],
        );

        assert(
            "with inner as (select * from table_1) select i.* from inner i",
            vec!["WILDCARD: i.*"],
        );
    }
}

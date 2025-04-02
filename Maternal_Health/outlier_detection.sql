drop table if exists "Maternal_Health_ETL".outlier_results;

do $$
declare
    col record;
    base_cte text;
    count_sql text;
    data_sql text;
    result_count int;
begin

    create table "Maternal_Health_ETL".outlier_results (
        column_name text,
        case_id int,
        value double precision
    );

    -- loop through all double precision columns
    for col in 
        select column_name
        from information_schema.columns
        where table_schema = 'Maternal_Health_ETL'
          and table_name = 'maternal_anthropometry'
          and data_type = 'double precision'
    loop
        base_cte := format(
            'with stats as (
                select
                    percentile_cont(0.25) within group (order by %1$I) as q1,
                    percentile_cont(0.75) within group (order by %1$I) as q3
                from "Maternal_Health_ETL".maternal_anthropometry
                where %1$I is not null
            ),
            bounds as (
                select
                    q1,
                    q3,
                    (q3 - q1) as iqr,
                    q1 - 3.0 * (q3 - q1) as lower_bound,
                    q3 + 3.0 * (q3 - q1) as upper_bound
                from stats
            )',
            col.column_name
        );

        count_sql := base_cte || format(
            ' select count(*) 
              from "Maternal_Health_ETL".maternal_anthropometry, bounds
              where %1$I < lower_bound or %1$I > upper_bound;',
            col.column_name
        );

        execute count_sql into result_count;

        data_sql := base_cte || format(
            ' insert into "Maternal_Health_ETL".outlier_results(column_name, case_id, value)
              select %L, case_id, %1$I
              from "Maternal_Health_ETL".maternal_anthropometry, bounds
              where %1$I < lower_bound or %1$I > upper_bound;',
            col.column_name, col.column_name
        );

        execute data_sql;
    end loop;
end $$;


select * from "Maternal_Health_ETL".outlier_results order by column_name, case_id;

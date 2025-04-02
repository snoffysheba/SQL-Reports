do $$
declare
    col record;
    dyn_sql text;
    record record;
    has_outliers boolean;
begin
    raise notice 'outlier pattern check (more than 2 digits after decimal):';

    for col in
        select column_name
        from information_schema.columns
        where table_schema = 'Maternal_Health_ETL'
          and table_name = 'maternal_anthropometry'
          and data_type = 'double precision'
    loop
        -- print the column being checked
        raise notice '--- % ---', col.column_name;

        -- check if any outlier exists for this column
        dyn_sql := format(
            'select exists (
                 select 1
                 from "Maternal_Health_ETL".maternal_anthropometry
                 where %1$I is not null
                   and length(split_part(%1$I::text, ''.'', 2)) > 2
             );',
            col.column_name
        );

        execute dyn_sql into has_outliers;

        if has_outliers then
            dyn_sql := format(
                'select case_id, %L as column_name, %1$I as value
                 from "Maternal_Health_ETL".maternal_anthropometry
                 where %1$I is not null
                   and length(split_part(%1$I::text, ''.'', 2)) > 2;',
                col.column_name
            );

            for record in execute dyn_sql loop
                raise notice 'case_id: %, value: %', record.case_id, record.value;
            end loop;
        else
            raise notice 'no outlier found';
        end if;
    end loop;
end $$;

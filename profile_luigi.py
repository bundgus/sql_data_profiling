from pyathena import connect
import csv
import datetime
import logging
import os
import sys
import traceback
import luigi
import json
from tabulate import tabulate

log = logging.getLogger("luigi-interface")

s3_staging_dir = 's3://s3-tn-dev-tnpai-shopping/query-results/mark/'
region_name = 'us-west-2'
conn = connect(s3_staging_dir=s3_staging_dir,
               region_name=region_name)


class ColumnTop100Values(luigi.Task):
    partitions = luigi.DictParameter()
    filters = luigi.ListParameter()
    schema = luigi.Parameter()
    table = luigi.Parameter()
    column = luigi.Parameter()

    def output(self):
        filename = ''
        for key in self.partitions:
            filename += self.partitions[key] + '-'
        filename = filename[:-1]
        filename += '_' + str(self.schema) + '.' + str(self.table) + '.' + str(self.column) + '.csv'
        filename = filename.replace('/', '_')
        output_path = os.path.join('profile_results', 'top100values', filename)
        return luigi.LocalTarget(output_path)

    def run(self):
        schema = str(self.schema)
        table = str(self.table)
        column = str(self.column)

        partition_where_clause = ''
        for key in self.partitions:
            partition_where_clause += 'and ' + key + " = '" + self.partitions[key] + "' "

        filter_where_clause = ''
        for filt in list(self.filters):
            filter_where_clause += 'and ' + filt + " "

        start = datetime.datetime.now()

        try:
            query = ('''
                       -- ColumnTop100Values
                       select 
                       'prod' as environment,
                       '{7}' as schema_name,
                       '{1}' as table_name,
                       '{0}' as column_name,
                       '{2}' as partition_where_clause,
                       '{6}' as filter_where_clause,
                       '{4}' as profile_start_timestamp,
                       {0} as value,
                       count(*) as row_count
                       from {1}
                       where 1=1
                       {5}
                       {3}
                       group by {0}
                       order by row_count desc
                       limit 100''').format(column,
                                            schema + '.' + table,
                                            str(partition_where_clause).replace("'", "''"),
                                            partition_where_clause,
                                            str(start),
                                            filter_where_clause,
                                            filter_where_clause.replace("'", "''"),
                                            schema)

            log.info('executing query')
            log.info(query)
            log.info('query start:' + str(start))

            with conn.cursor() as cur:
                cur.execute(query)
                if cur.state != 'SUCCEEDED':
                    raise Exception('query failed: ' + query)
                result = cur.fetchall()
                col_names = [i[0] for i in cur.description]

            end = datetime.datetime.now()
            log.info('query end: ' + str(end))
            log.info('query elapsed: ' + str(end - start))

            log.info(tabulate(result, headers=col_names, tablefmt='psql'))

            filename = ''
            for key in self.partitions:
                filename += self.partitions[key] + '-'
            filename = filename[:-1]
            filename += '_' + str(self.schema) + '.' + str(self.table) + '.' + str(self.column) + '.csv'
            filename = filename.replace('/', '_')
            output_path = os.path.join('profile_results', 'top100values', filename)

            with open(output_path, 'w', newline='') as csvfile:
                csvwriter = csv.writer(csvfile, quoting=csv.QUOTE_MINIMAL)
                csvwriter.writerow(col_names)
                csvwriter.writerows(result)
        except Exception as e:
            log.info(e)
            traceback.print_exc(file=sys.stdout)


class ProfileStringColumns(luigi.Task):
    partitions = luigi.DictParameter()
    filters = luigi.ListParameter()
    schema = luigi.Parameter()
    table = luigi.Parameter()
    column = luigi.Parameter()

    def output(self):
        filename = ''
        for key in self.partitions:
            filename += self.partitions[key] + '-'
        filename = filename[:-1]
        filename += '.' + str(self.schema) + '.' + str(self.table) + '.' + str(self.column) + '.csv'
        output_path = os.path.join('profile_results', 'string_column_profiles', filename)
        return luigi.LocalTarget(output_path)

    def run(self):
        schema = str(self.schema)
        table = str(self.table)
        column = str(self.column)

        partition_where_clause = ''
        for key in self.partitions:
            partition_where_clause += 'and ' + key + " = '" + self.partitions[key] + "' "

        filter_where_clause = ''
        for filt in self.filters:
            filter_where_clause += 'and ' + filt + " "

        start = datetime.datetime.now()

        try:
            query = '''
                -- ProfileStringColumns
                select 
                'prod' as environment,
                '{7}' as schema_name,
                '{1}' as table_name,
                '{0}' as column_name,
                '{2}' as partition_where_clause,
                '{6}' as filter_where_clause,
                '{4}' as profile_start_timestamp,
                'string' as column_type,
                min(length({0})) as text_min_length,
                max(length({0})) as text_max_length,
                avg(length({0})) as text_mean_length,
                count({0}) as column_non_null_row_count,
                count(*) as column_row_count
                from {1}
                where 1=1
                {5}
                {3}
            '''.format(column,
                       schema + '.' + table,
                       str(partition_where_clause).replace("'", "''"),
                       partition_where_clause,
                       str(start),
                       filter_where_clause,
                       filter_where_clause.replace("'", "''"),
                       schema)

            log.info('executing query')
            log.info(query)
            log.info('query start:' + str(start))

            with conn.cursor() as cur:
                cur.execute(query)
                if cur.state != 'SUCCEEDED':
                    raise Exception('query failed: ' + query)
                result = cur.fetchall()
                col_names = [i[0] for i in cur.description]

            end = datetime.datetime.now()
            log.info('query end: ' + str(end))
            log.info('query elapsed: ' + str(end - start))

            log.info(tabulate(result, headers=col_names, tablefmt='psql'))

            filename = ''
            for key in self.partitions:
                filename += self.partitions[key] + '-'
            filename = filename[:-1]
            filename += '.' + str(self.schema) + '.' + str(self.table) + '.' + str(self.column) + '.csv'
            filename = filename.replace('/', '_')
            output_path = os.path.join('profile_results', 'string_column_profiles', filename)

            with open(output_path, 'w', newline='') as csvfile:
                csvwriter = csv.writer(csvfile, quoting=csv.QUOTE_MINIMAL)
                csvwriter.writerow(col_names)
                csvwriter.writerows(result)
        except Exception as e:
            log.info(e)
            traceback.print_exc(file=sys.stdout)


class ProfileNumberColumns(luigi.Task):
    partitions = luigi.DictParameter()
    filters = luigi.ListParameter()
    schema = luigi.Parameter()
    table = luigi.Parameter()
    column = luigi.Parameter()

    def output(self):
        filename = ''
        for key in self.partitions:
            filename += self.partitions[key] + '-'
        filename = filename[:-1]
        filename += '.' + str(self.schema) + '.' + str(self.table) + '.' + str(self.column) + '.csv'
        output_path = os.path.join('profile_results', 'number_column_profiles', filename)
        return luigi.LocalTarget(output_path)

    def run(self):
        schema = str(self.schema)
        table = str(self.table)
        column = str(self.column)

        partition_where_clause = ''
        for key in self.partitions:
            partition_where_clause += 'and ' + key + " = '" + self.partitions[key] + "' "

        filter_where_clause = ''
        for filt in self.filters:
            filter_where_clause += 'and ' + filt + " "

        start = datetime.datetime.now()

        try:
            query = '''
                -- ProfileNumberColumns
                select 
                'prod' as environment,
                '{7}' as schema_name,
                '{1}' as table_name,
                '{0}' as column_name,
                '{2}' as partition_where_clause,
                '{6}' as filter_where_clause,
                '{4}' as profile_start_timestamp,
                'number' as column_type,
                min({0}) as number_min_value,
                max({0}) as number_max_value,
                avg({0}) as number_mean_value,
                count({0}) as column_non_null_row_count,
                count(*) as column_row_count
                from {1}
                where 1=1
                {5}
                {3}
            '''.format(column,
                       schema + '.' + table,
                       str(partition_where_clause).replace("'", "''"),
                       partition_where_clause,
                       str(start),
                       filter_where_clause,
                       filter_where_clause.replace("'", "''"),
                       schema)

            log.info('executing query')
            log.info(query)
            log.info('query start:' + str(start))

            with conn.cursor() as cur:
                cur.execute(query)
                if cur.state != 'SUCCEEDED':
                    raise Exception('query failed: ' + query)
                result = cur.fetchall()
                col_names = [i[0] for i in cur.description]

            end = datetime.datetime.now()
            log.info('query end: ' + str(end))
            log.info('query elapsed: ' + str(end - start))

            log.info(tabulate(result, headers=col_names, tablefmt='psql'))

            filename = ''
            for key in self.partitions:
                filename += self.partitions[key] + '-'
            filename = filename[:-1]
            filename += '.' + str(self.schema) + '.' + str(self.table) + '.' + str(self.column) + '.csv'
            filename = filename.replace('/', '_')
            output_path = os.path.join('profile_results', 'number_column_profiles', filename)

            with open(output_path, 'w', newline='') as csvfile:
                csvwriter = csv.writer(csvfile, quoting=csv.QUOTE_MINIMAL)
                csvwriter.writerow(col_names)
                csvwriter.writerows(result)
        except Exception as e:
            log.info(e)
            traceback.print_exc(file=sys.stdout)


# root task
class ProfileAllColumns(luigi.Task):
    iscomplete = False

    def complete(self):
        return self.iscomplete

    def requires(self):
        required = []

        # start with list of all tables to be profiled and what partition values to filter
        # load table configuration from table_list.json
        with open('profile_config.json') as json_file:
            tables = json.load(json_file)

        # for each table get a list of columns and their types
        for table in tables:

            partition_columns = table['partitions'].keys()

            query = f'''
                SELECT 
                column_name,
                data_type
                FROM information_schema.columns 
                WHERE table_schema = '{table['schema_name']}'
                AND table_name='{table['table_name']}'
            '''

            log.info(query)
            with conn.cursor() as cur:
                cur.execute(query)
                if cur.state != 'SUCCEEDED':
                    raise Exception('query failed: ' + query)
                result = cur.fetchall()

            log.info(result)

            string_columns = []
            number_columns = []
            for r in result:
                if r[1] is None:  # end of column list in describe output
                    break
                if r[0] not in partition_columns:  # don't profile the partition columns
                    if r[1] == 'string' or r[1][:4] == 'char' or r[1][:7] == 'varchar':
                        log.info(f'found string column: {r[0]}')
                        string_columns.append(r[0])
                    if r[1] in ['integer', 'int', 'bigint', 'double']:
                        log.info(f'found number column: {r[0]}')
                        number_columns.append(r[0])

            all_columns = string_columns + number_columns
            ignore_all = table['ignore_all']

            # for each column, get top 100 values by count
            ignore_top_100 = table['ignore_top_100']
            for col in all_columns:
                if col not in ignore_all and col not in ignore_top_100:
                    required.append(ColumnTop100Values(partitions=table['partitions'],
                                                       filters=table['filters'],
                                                       schema=table['schema_name'],
                                                       table=table['table_name'],
                                                       column=col))
                else:
                    print('ignoring column for top 100 profiling: ' + col)

            # get column level profiling for text types
            for col in string_columns:
                if col not in ignore_all:
                    required.append(ProfileStringColumns(partitions=table['partitions'],
                                                         filters=table['filters'],
                                                         schema=table['schema_name'],
                                                         table=table['table_name'],
                                                         column=col))
                else:
                    print('ignoring column for number column profiling: ' + col)

            # get column level profiling for number types
            for col in number_columns:
                if col not in ignore_all:
                    required.append(ProfileNumberColumns(partitions=table['partitions'],
                                                         filters=table['filters'],
                                                         schema=table['schema_name'],
                                                         table=table['table_name'],
                                                         column=col))
                else:
                    print('ignoring column for number column profiling: ' + col)

        return required

    def run(self):
        self.iscomplete = True

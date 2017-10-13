import luigi
import logging
import pandas as pd
import networkx as nx
import string
import htetl.get_data as get_data
import htetl.reverse_url as reverse_url
import htetl.tasks.emails
import htetl.tasks.phones
import htetl.util as util


logger = logging.getLogger('luigi-interface')
logger.setLevel(logging.DEBUG)


class MakeGraph(luigi.Task):
    """  reversing url task """

    NEW_DATA_EXTRACTOR = luigi.BoolParameter(
        config_path=dict(section='htetl-flags', name='new_data_extractor'),
        default=False
    )

    def output(self):
        out_path = 'data/graph.csv'
        return luigi.LocalTarget(out_path)

    def requires(self):
        if self.NEW_DATA_EXTRACTOR:
            return {
                'email': htetl.tasks.emails.ParseEmails(),
                'phone': htetl.tasks.phones.ParsePhones(),
                # TODO: This needs to be adapted to the new Page data
                # extraction methods like ParseEmail and ParsePhones
                'oid': get_data.RawPosterData(),
            }
        else:
            return [get_data.RawPhoneData(),
                    get_data.RawEmailData(),
                    get_data.RawPosterData()]

    def run(self):
        """ data is small enough to use pandas for the processing """

        graphs = []

        # TODO: When htetl-flags:new_data_extractor is removed, remove the
        # flatten call and clean up the for loop
        for in_target in luigi.task.flatten(self.input()):
            with in_target.open('r') as f:
                data = pd.read_csv(f)

            for i, (k, v) in enumerate(data.groupby(data.columns[-1])):
                # if there is more than one page, links all pages by
                # offsetting, like:
                #
                # [ 1 2 3 ]
                # [ 2 3 1 ]
                #
                # becomes [ (1, 2), (2, 3), (3, 1) ]
                values = v.values.tolist()
                page_ids = [x[0] for x in values]
                offset_page_ids = page_ids[1:]
                if len(page_ids) == 1:
                    offset_page_ids = page_ids
                else:
                    offset_page_ids.append(page_ids[0])
                graphs.append([
                    (a, b)
                    for a, b in zip(page_ids, offset_page_ids)
                ])

        edges = [edge for edge_list in graphs for edge in edge_list]

        G = nx.Graph()
        G.add_edges_from(edges)

        sub_graphs = []
        page_graph_id = []
        for i, x in enumerate(nx.connected_component_subgraphs(G)):
            nodes = nx.nodes(x)
            page_graph_id.extend([(i, node) for node in nodes])

        df_out = pd.DataFrame(
            page_graph_id,
            columns=['entity_id', 'backpagepostid']
        ).astype(int)

        with self.output().open('w') as f:
            df_out.to_csv(f, index=None)


class LoadEntityIds(util.LoadPostgres):
    table = 'backpageentities'

    columns = [('entity_id', 'int'),
               ('backpagepostid', 'int')]

    header = True

    def requires(self):
        return MakeGraph()

class ParseEmails(luigi.Task):
    '''
        Parses emails from raw Backpage posts & saves emails / post IDs in CSV file
        NB: only finds the FIRST email in the posting (if any)
    '''
    host = luigi.Parameter(significant=False)
    database = luigi.Parameter(significant=False)
    user = luigi.Parameter(significant=False)
    password = luigi.Parameter(significant=False)
    outfile = 'data/parsed_email.csv'

    def requires(self):
        return get_data.RawHTMLPostData(self.host, self.database, self.user, self.password)

    def output(self):
        return luigi.LocalTarget(self.outfile)

    def run(self):
        in_path = self.input().path
        logger.info("Processing {}".format(in_path))
        df = pd.read_csv(in_path)
        # (...) = make a capture group - at least one is required for pandas Series.str.extract method
        # [\w._-] = match any alphanumeric character (\w) or . or _ or -
        # + = match preceding expression one or more times
        # @ = match @
        # \. = match .
        df["email"] = df["body"].str.extract("([\w._-]+@[\w_-]+\.\w+)", expand=True)
        df = df.drop('body', 1)
        df = df.dropna(axis=0, how='any')
        if not df["email"].str.extract('(,)', expand=True).dropna(axis=0, how='any').empty:
            raise ValueError(' '.join(['At least one parsed email address contains a comma,',
                             'which may cause problems when other code loads', self.outfile, 'with comma separator']))

        with open(self.output().path, 'a') as f:  # write posting id & emails to CSV
            df.to_csv(f, index=None, encoding='utf-8')

class EmailsToPostgres(util.LoadPostgres):
    '''
        Loads CSV file of parsed emails / posting ids and saves to Postgres table.
        NB: The way luigi.postgres.CopyToTable is set up, if you run this
        twice in a row it won't overwrite the existing table. To make it save
        a new table, in Postgres command line or pgAdmin you have to drop that table
        AND the table called "table_updates" (or at least the "Emails_to_Postgres" row).
        Otherwise Luigi will think the Task is already done, because it checks "table_updates".
    '''
    header = True
    table = 'emailaddress' # safer to make table / column names lowercase
    columns = [("backpagepostid", "INT"),
               ("email",          "TEXT")]

    def requires(self):
        return ParseEmails(self.host, self.database, self.user, self.password)


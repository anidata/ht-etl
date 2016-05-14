import luigi
import logging
import pandas as pd
import networkx as nx
from . import get_data
from . import reverse_url
from . import util

logger = logging.getLogger('luigi-interface')

logger.setLevel(logging.DEBUG)


class HtTasks(luigi.WrapperTask):
    """ Runs all tasks """

    def requires(self):
        yield get_data.RawEmailData()
        yield get_data.RawPhoneData()
        yield reverse_url.LoadReverseUrl()


class MakeGraph(luigi.Task):
    """  reversing url task """

    def output(self):
        out_path = 'data/graph.csv'
        return luigi.LocalTarget(out_path)

    def requires(self):
        return [get_data.RawPhoneData(),
                get_data.RawEmailData(),
                get_data.RawPosterData()]

    def run(self):
        """ data is small enough to use pandas for the processing """

        out = []
        for in_path in self.input():
            data = pd.read_csv(in_path.path)
            logger.info("Processing {}".format(in_path.path))

            for i, (k, v) in enumerate(data.groupby(data.columns[-1])):
                v = v.values.tolist()
                v = [x[0] for x in v]
                v_right = v[1:]
                if len(v) == 1:
                    v_right = v
                else:
                    v_right[-1] = v[0]
                out.append([(a, b) for a, b in zip(v, v_right)])

        out = [item for sublist in out for item in sublist]

        logger.info("Making Graph".format(in_path.path))
        G = nx.Graph()
        G.add_edges_from(out)

        sub_graphs = []
        for i, x in enumerate(nx.connected_component_subgraphs(G)):
            nodes = nx.nodes(x)
            sub_graphs.append(list(zip([i] * len(nodes), nodes)))

        sub_graphs = [item for sublist in sub_graphs for item in sublist]

        df_out = pd.DataFrame(sub_graphs,
                              columns=['entity_id',
                                       'backpagepostid']).astype(int)
        df_out.to_csv(self.output().path, index=None)


class LoadEntityIds(util.LoadPostgres):
    table = 'backpageentities'

    columns = [('entity_id', 'int'),
               ('backpagepostid', 'int')]

    header = True

    def requires(self):
        return MakeGraph()

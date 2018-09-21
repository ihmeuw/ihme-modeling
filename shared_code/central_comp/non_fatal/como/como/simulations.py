import os
import numpy as np
import pandas as pd

from como.cython_modules import fast_random


os.umask(0o0002)  # Set dUSERt file mask to readable-for all users


class IndependentComorbidity(object):

    def __init__(self, sim_df, disability_weights_df, draw_cols, agg_cause_map
                 ):

        # sorting the indices is absolutely vital
        if sim_df["sequela_id"].duplicated().any():
            raise ValueError("sim_df must be unique by sequela_id")
        sim_df = sim_df.set_index("sequela_id").sort_index()

        if disability_weights_df["sequela_id"].duplicated().any():
            raise ValueError("sim_df must be unique by sequela_id")
        disability_weights_df = disability_weights_df.set_index(
            "sequela_id").sort_index()

        if sim_df.shape != disability_weights_df.shape:
            raise ValueError("sim_df and disability_weights_df must contain"
                             " the same sequela and draws")

        # inputs
        self.sim_df = sim_df
        self.skip_df = None
        self.sim_dws_df = disability_weights_df
        self.skip_dws_df = None
        self.draw_cols = draw_cols
        self.agg_cause_map = agg_cause_map

        # simulation results
        self._comos = []
        self._ylds = []
        self._dw_counts = []
        self._sim_people = []
        self._agg_causes = []

        # Set random seed
        np.random.seed()

    @property
    def comos(self):
        comos_out = pd.DataFrame({'num_diseases': range(100)})
        return comos_out.join(pd.concat(self._comos, axis=1))

    @property
    def ylds(self):
        ylds = pd.DataFrame()
        if self._ylds:
            sim_ylds = pd.DataFrame(index=self.sim_df.index).reset_index()
            sim_ylds = sim_ylds.join(pd.concat(self._ylds, axis=1))
            ylds = ylds.append(sim_ylds)

        if self.skip_df is not None and not self.skip_df.empty:
            skip_ylds = (self.skip_df[self.draw_cols] *
                         self.skip_dws_df[self.draw_cols])
            ylds = ylds.append(skip_ylds.reset_index())

        return ylds

    @property
    def agg_causes(self):
        df = pd.concat(self._agg_causes, axis=1)
        df.index.rename("cause_id", inplace=True)
        return df.reset_index()

    def compute_skips(self, skip_threshold):
        # compute mean that we apply skip logic to
        w_mean = self.sim_df.copy()
        w_mean['mean'] = self.sim_df[self.draw_cols].mean(axis=1)

        sim_df = w_mean[self.draw_cols]
        self.skip_df = sim_df[w_mean['mean'] < skip_threshold]
        self.sim_df = sim_df[w_mean['mean'] >= skip_threshold]

        dws_df = self.sim_dws_df.copy()
        self.skip_dws_df = dws_df[dws_df.index.isin(self.skip_df.index)]
        self.sim_dws_df = dws_df[dws_df.index.isin(self.sim_df.index)]

    def _track_ylds(self, sim_people, sim_dws_mat, n_simulants, draw_num):
        # Calculate combined disability weight of each simulant as:
        #    1 - (the product of (1 - each dw)).
        dw_sim = sim_dws_mat[:, draw_num] * sim_people
        combined_dw = 1 - np.prod((1 - dw_sim), axis=1)

        # Attribute the combined dw back to each constituent disease
        denom = np.sum(dw_sim, axis=1)
        denom[denom == 0] = 1
        yld_sim = (
            (dw_sim / denom.reshape(denom.shape[0], 1)) *
            combined_dw.reshape(combined_dw.shape[0], 1))
        yld_rate = np.sum(yld_sim, axis=0) / n_simulants

        self._ylds.append(
            pd.DataFrame(data={'draw_' + str(draw_num): yld_rate}))

    def _track_aggregate_causes(self, sim_people, draw_num):
        cause_prev = []
        for cause_id, cframe in self.agg_cause_map.groupby('cause_id'):
            cmask = self.sim_df.index.isin(cframe.sequela_id).astype(int)
            cbool = np.where(cmask == 1)[0]
            prev = (np.count_nonzero(sim_people[:, cbool].sum(axis=1)) /
                    float(sim_people.shape[0]))
            df = pd.DataFrame(
                data={'draw_{}'.format(str(draw_num)): prev},
                index=[cause_id])
            cause_prev.append(df)
        self._agg_causes.append(pd.concat(cause_prev))

    def _track_comos(self, sim_people, draw_num):
        """Keep track of # of comorbidities for diagnostic purposes"""
        # assume no simulant has > than 100 COMOs
        num_diseases_each = np.sum(sim_people, axis=1, dtype=np.uint32)
        comorbidities = np.zeros(100)
        disease_counts = np.bincount(num_diseases_each)
        for i in range(len(disease_counts)):
            comorbidities[i] = disease_counts[i]
        self.comos.append(pd.DataFrame(
            data={'num_people_' + str(draw_num): comorbidities}))

    def _track_disability_distribution(self, sim_people, sim_dws_mat, num_bins,
                                       draw_num):
        # Calculate combined disability weight of each simulant as:
        #    1 - (the product of (1 - each dw)).
        dw_sim = sim_dws_mat[:, draw_num] * sim_people
        combined_dw = 1 - np.prod((1 - dw_sim), axis=1)

        bins = np.linspace(0, 1, num_bins + 1)
        binned = np.digitize(combined_dw, bins)
        true_zeros = [len(combined_dw[combined_dw == 0])]

        # Digitize start its bin count at 1, so when using the subsequent
        # bincount function, skip the 0th bin. Also make sure minimum length
        # (again, omitting the 0th bincount) is equal to the number of bins
        distribution = np.bincount(binned, minlength=num_bins + 1)[1:]

        true_zeros.extend(distribution)
        distribution = true_zeros
        bin_labels = [-1]
        bin_labels.extend(bins[0:num_bins])
        distribution = pd.DataFrame({
            'bin_id': range(len(bin_labels)),
            'bin_lower': bin_labels,
            'draw_{}'.format(str(draw_num)): distribution})

        self._dw_counts.append(distribution)

    def _track_sequela_by_simulant(self, sim_people, sim_dws_mat, draw_num):
        """Store the sequela that each simulant has... storage
        requirements are quite high, probably want to avoid unless
        absolutely necessary"""
        dw_sim = sim_dws_mat[:, draw_num] * sim_people
        combined_dw = 1 - np.prod((1 - dw_sim), axis=1)
        ss = sim_people * self.sim_df.index.values
        simulant_sequelae = [
            ";".join(np.nonzero(row)[0].astype('str')) for row in ss]
        df = pd.DataFrame(
            data={'sequelae_' + str(draw_num): simulant_sequelae,
                  'dw_' + str(draw_num): combined_dw})
        self._sim_people.append(df)

    def simulate(self, n_simulants, ylds=True, agg_causes=True,
                 n_all_cause_draws=100, comos=False,
                 disability_distribution=False, sequela_by_simulant=False):

        # dUSERt skip logic
        if self.skip_df is None:
            self.compute_skips(2. / n_simulants)

        sim_mat = self.sim_df.reset_index()[self.draw_cols].as_matrix()
        sim_dws_mat = self.sim_dws_df.reset_index()[self.draw_cols].as_matrix()

        for draw_num in range(len(self.draw_cols)):
            print("simulating draw: {}".format(draw_num))
            # simulate each person
            sim_people = fast_random.bernoulli(
                n_simulants, sim_mat[:, draw_num])

            if ylds:
                self._track_ylds(
                    sim_people, sim_dws_mat, n_simulants, draw_num)
            if agg_causes:
                if draw_num <= n_all_cause_draws:
                    self._track_aggregate_causes(sim_people, draw_num)
            if comos:
                self._track_comos(sim_people, draw_num)
            if disability_distribution:
                self._track_disability_distribution(
                    sim_people, sim_dws_mat, 20, draw_num)
            if sequela_by_simulant:
                self._track_sequela_by_simulant(sim_people, sim_dws_mat)


class DependentComorbidity(object):
    pass

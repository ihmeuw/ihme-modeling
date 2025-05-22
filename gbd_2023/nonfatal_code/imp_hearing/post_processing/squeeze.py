"""
Project: GBD Hearing Loss
Purpose: Classes and functions for the parent and etiology squeezes. The code
present in this script is run in process.py.
"""

import logging
import sys

import numpy as np
import pandas as pd

from modeling import config as cfg

from core.aggregation import (aggregate_causes)
from core.transform import (fill_draws,
                       merge_draws,
                       transform_draws)
from core.quality_checks import (check_df_for_nulls,
                            check_shape,
                            merge_check,
                            check_occurances_for_df)


class Squeeze(object):
    """Base squeeze class"""

    def __init__(self):
        pass

    def fit_estimates(self,
                      estimates,
                      prop,
                      merge_cols,
                      draw_count):
        """
        Take estimates for a number of related models and fit them into their
        respective envelope by applying a proportion.

        Arguments:
            estimates (dataframe): Pandas dataframe of draws representing
                several models.
            prop (dataframe): Dataframe of proportions used to fit model
                estimates into the envelope.
            merge_cols (strlist): The column or columns to merge two dataframe
                of draws on for draw level transformations.
            draw_count (int): The number of draws in the dataframe, .

        Returns:
            Adjusted estimates that have been "fit" into the envelope.
        """
        logging.info(
            "Multiplying estimates by the specified proportion to fit by")
        adjusted_estimates = transform_draws(
            'mul',
            df1=estimates,
            df2=prop,
            merge_cols=merge_cols,
            draw_count=draw_count)

        return adjusted_estimates


class ParentSqueeze(Squeeze):
    """Template class for creating and executing a parent squeeze."""

    def __init__(self):
        Squeeze.__init__(self)

    def calc_total_prop(self,
                        estimates,
                        id_cols,
                        val_cols,
                        merge_cols,
                        draw_count):
        """
        Take estimates for multiple models and calculate the proportion that
        each model comprises of the total of all estimates.

        Arguments:
            estimates (dataframe): Pandas dataframe of draws representing
                several models.
            id_cols (strlist): Dimensions to aggregate by (e.g.
                modelable_entity_id, sex_id and age_group_id).
            val_cols (strlist): The column or columns to sum values for.
            merge_cols (strlist): The column or columns to merge two dataframe
                of draws on for draw level transformations.
            draw_count (int): The number of draws in the dataframe.

        Returns:
            Dataframe of proportion draws for each model.

        """
        # Calculate total
        total_df = aggregate_causes(estimates, id_cols, val_cols)

        # Divid 1 by the total
        numerator = fill_draws(df=total_df,
                               fill_val=1,
                               draw_col='draw_',
                               draw_count=draw_count)
        check_occurances_for_df(df=numerator, cols=['age_group_id',
                                                    'sex_id',
                                                    'location_id',
                                                    'year_id'])
        check_shape(df=numerator)
        numerator = check_df_for_nulls(df=numerator,
                                       entity_name="Parent squeeze numerator")

        squeeze_prop = transform_draws('truediv',
                                       df1=numerator,
                                       df2=total_df,
                                       merge_cols=merge_cols,
                                       draw_count=draw_count)
        squeeze_prop = check_df_for_nulls(df=squeeze_prop,
                                          entity_name="parent squeeze "
                                                      "proportion")

        return squeeze_prop

    def squeeze_envelopes(self,
                          estimates,
                          id_cols,
                          val_cols,
                          merge_cols,
                          draw_count):
        """
        Execute a parent squeeze by calculating the proportion each envelope
        makes up of the sum of all given envelopes and fitting the envelopes
        into the total by
        applying the proportions across the envelopes.

        Arguments:
            estimates (dataframe): Pandas dataframe of estimates representing
                the severity specific envelopes to squeeze.
            id_cols (strlist): Dimensions to aggregate by (e.g.
                modelable_entity_id, sex_id and age_group_id).
            val_cols (strlist): The column or columns to sum values for.
            merge_cols (strlist): The column or columns to merge two dataframe
                of draws on for draw level transformations.
            draw_count (int): The number of draws in the dataframe, .

        Returns:
            Dataframe of squeezed severity specific envelopes.

        """
        # For each envelope, multiply by 1 divided by the total prevalence
        # (env_draw * (1 / total)
        prop = self.calc_total_prop(estimates=estimates,
                                    id_cols=id_cols,
                                    val_cols=val_cols,
                                    merge_cols=merge_cols,
                                    draw_count=draw_count)

        # Multiply envelopes by the quotient
        squeezed_estimates = self.fit_estimates(estimates=estimates,
                                                prop=prop,
                                                merge_cols=merge_cols,
                                                draw_count=draw_count)

        return squeezed_estimates


class EtiologySqueeze(Squeeze):
    """Template class for creating and executing an etiology squeeze"""

    def __init__(self):
        Squeeze.__init__(self)

    def calc_etiology_prop(self,
                           estimates,
                           numerator,
                           id_cols,
                           val_cols,
                           merge_cols,
                           draw_count):
        """
        Take estimates for multiple models representing etiologies and
        calculates the proportion that each model comprises of the total of
        all specified etiologies.

        Arguments:
            estimates (dataframe): Pandas dataframe of draws representing
                several models that will be summed to create a denominator
                for the proportion calculation.
            numerator (dataframe): Pandas dataframe of draws representing
                models that will be used as the numerator in the proportion
                calculation.
            id_cols (strlist): Dimensions to aggregate by (e.g.
                modelable_entity_id, sex_id and age_group_id).
            val_cols (strlist): The column or columns to sum values for.
            merge_cols (strlist): The column or columns to merge two dataframe
                of draws on for draw level transformations.
            draw_count (int): The number of draws in the dataframe.

        Returns:
            Dataframe of proportion draws for each model.

        """
        # Calculate total
        total_df = aggregate_causes(estimates, id_cols, val_cols)

        # Divid numerator by the total
        squeeze_prop = transform_draws('truediv',
                                       df1=numerator,
                                       df2=total_df,
                                       merge_cols=merge_cols,
                                       draw_count=draw_count,
                                       cols=val_cols,
                                       fill_nulls=True,
                                       fill_val=0)

        return squeeze_prop

    def squeeze_etiologies(self,
                           estimates_to_fit,
                           prop_numerator,
                           prop_denomenator,
                           id_cols,
                           val_cols,
                           merge_cols,
                           draw_count):
        """
        Squeeze all etiologies by summing the etiologies to obtain the total
        and then calculating the proportion of the total

        Arguments:
            estimates_to_fit (dataframe): Pandas dataframe of draws
                representing envelope estimates that will be used to squeeze
                the etiologies into.
            prop_numerator (dataframe): Pandas dataframe of draws representing
                models that will be used as the numerator in the proportion
                calculation.
            prop_denomenator (dataframe): Pandas dataframe of draws
                representing several models that will be summed to create a
                denominator for the proportion calculation.
            id_cols (strlist): Dimensions to aggregate by (e.g.
                modelable_entity_id, sex_id and age_group_id).
            val_cols (strlist): The column or columns to sum values for.
            merge_cols (strlist): The column or columns to merge two dataframe
                of draws on for draw level transformations.
            draw_count (int): The number of draws in the dataframe, .

        Returns:
            Dataframe of draws where the proportion of severity specific
            etiologies over the sum of all etiologies has been applied to the
            severity specific envelopes.

        """
        # Obtain the proportion of a severity specific etiology over the sum
        # of all etiologies
        prop = self.calc_etiology_prop(estimates=prop_denomenator,
                                       numerator=prop_numerator,
                                       id_cols=id_cols,
                                       val_cols=val_cols,
                                       merge_cols=merge_cols,
                                       draw_count=draw_count)
        nondraw_cols = set(prop.columns) - set(prop.filter(like='draw').columns)
        logging.info(nondraw_cols)

        # Multiply envelopes by the quotient
        squeezed_estimates = self.fit_estimates(
            estimates=estimates_to_fit,
            prop=prop,
            merge_cols=merge_cols,
            draw_count=draw_count)
        nondraw_cols = set(squeezed_estimates.columns) - \
                       set(squeezed_estimates.filter(like='draw').columns)
        logging.info(nondraw_cols)

        return squeezed_estimates


class HearingLossParentSqueeze(ParentSqueeze):
    """Class for executing a parent squeeze for hearing loss"""

    def __init__(self):
        ParentSqueeze.__init__(self)

    def run(self, envelopes, id_cols, val_cols, merge_cols, draw_count):
        """
        Function that acts as a main method and calls methods for each step
        of the etiology squeeze.

        Arguments:
            envelopes (dataframe): Pandas dataframe of estimates representing
                the hearing loss envelopes.
            id_cols (strlist): Dimensions to aggregate by (e.g.
                modelable_entity_id, sex_id and age_group_id).
            val_cols (strlist): The column or columns to sum values for.
            merge_cols (strlist): The column or columns to merge two dataframe
                of draws on for draw level transformations.
            draw_count (int): The number of draws in the dataframe, .

        Returns:
            Estimates for each, etiology, severity and demographic dimension
            that have been squeezed into the respective severity specific
            envelope.
        """

        # 1). Squeeze moderate to complete hearing loss (35+), normal hearing (
        # 0-19), and mild loss hearing (20-34) to sum up to 1.

        squeezed_estimates = []
        norm_com = envelopes.copy()
        mod_sev_com = envelopes.copy()
        norm_com = norm_com[
            norm_com['severity'].isin(['norm', 'mld', 'mod_com'])]
        mod_sev_com = mod_sev_com[~mod_sev_com['severity'].isin(['norm',
                                                                 'mld',
                                                                 'mod_com'])]
        norm_com = self.squeeze_envelopes(estimates=norm_com,
                                          id_cols=id_cols,
                                          val_cols=val_cols,
                                          merge_cols=merge_cols,
                                          draw_count=draw_count)
        norm_com = check_df_for_nulls(df=norm_com,
                                      entity_name='mild to complete envelope '
                                                  'draws')
        squeezed_mld_mod = norm_com.copy()
        squeezed_mld_mod = squeezed_mld_mod[
            squeezed_mld_mod['severity'].isin(['mld'])]
        squeezed_mld_mod = check_df_for_nulls(df=squeezed_mld_mod,
                                              entity_name='squeezed mild and '
                                                          'moderate envelope '
                                                          'draws')
        squeezed_norm = norm_com.copy()
        squeezed_norm = squeezed_norm[squeezed_norm['severity'] == 'norm']
        squeezed_estimates.append(squeezed_mld_mod)

        squeezed_mod_sev_com_agg = norm_com.copy()
        squeezed_mod_sev_com_agg = squeezed_mod_sev_com_agg[
            squeezed_mod_sev_com_agg['severity'] == 'mod_com']
        squeezed_mod_sev_com_agg = check_df_for_nulls(
            df=squeezed_mod_sev_com_agg,
            entity_name='envelope draws')
        squeezed_mod_sev_com_agg = squeezed_mod_sev_com_agg.drop(
            labels=['severity'],
            axis=1)

        # 2). Squeeze categorical results into moderate to complete hearing
        # loss (35+) parent envelope. 
        mod_sev_com = self.squeeze_envelopes(estimates=mod_sev_com,
                                             id_cols=id_cols,
                                             val_cols=val_cols,
                                             merge_cols=merge_cols,
                                             draw_count=draw_count)
        mod_sev_com = check_df_for_nulls(df=mod_sev_com,
                                         entity_name='squeezed moderately '
                                                     'severe to complete')
        mod_sev_com = self.fit_estimates(estimates=mod_sev_com,
                                         prop=squeezed_mod_sev_com_agg,
                                         merge_cols=merge_cols,
                                         draw_count=draw_count)
        mod_sev_com = check_df_for_nulls(df=mod_sev_com,
                                         entity_name='squeezed moderately '
                                                     'severe to complete')
        squeezed_estimates.append(mod_sev_com)
        squeezed_envelopes = pd.concat(squeezed_estimates)
        squeezed_envelopes = check_df_for_nulls(df=squeezed_envelopes,
                                                entity_name='squeezed '
                                                            'envelopes')

        # Check to ensure parent squeeze was executed correctly
        logging.info("Confirming squeeze was done correctly")
        total_envs = squeezed_envelopes.copy()
        total_envs = pd.concat([total_envs, squeezed_norm])
        total_envs = aggregate_causes(df=total_envs,
                                      id_cols=id_cols,
                                      val_cols=val_cols)
        sum_total = fill_draws(df=total_envs, fill_val=1)

        ps_check_df = merge_draws(left_df=total_envs,
                                  right_df=sum_total,
                                  merge_cols=id_cols,
                                  suffixes=['_te', '_st'])

        for i in range(draw_count):
            if not np.allclose(ps_check_df['draw_{}_te'.format(i)],
                               ps_check_df['draw_{}_st'.format(i)]):
                raise ValueError("""The sum of all of the severity specific
                                    envelopes is NOT equal to 1 in column {}
                                 """.format(i))

        logging.info("Squeeze is successfully working")

        return squeezed_envelopes


class HearingLossEtiologySqueeze(EtiologySqueeze):
    """Class for executing an etiology squeeze for hearing loss"""

    def __init__(self):
        EtiologySqueeze.__init__(self)

    def make_congenital(self, envelopes):
        """
        Prevalence at birth assumed to be all congenital. Prevalence remains
        constant (no excess mortality or remission).

        Arguments:
            envelopes (dataframe): Pandas dataframe of estimates representing
            the hearing loss envelopes.

        Returns:
            Dataframe of estimates that have been subsetted to birth prevalence
            estimates (age_group_id ID).
        """
        age_df_list = []

        congenital = envelopes.copy()
        congenital = congenital[congenital['age_group_id'] == ID]
        congenital['entity'] = 'cong'
        age_df_list.append(congenital)

        # Populate other age groups with Congenital estimates
        logging.info("Filling estimates for other age groups with "
                     "congenital estimates")
        other_age_groups = envelopes.copy()
        other_age_groups = other_age_groups[
            other_age_groups['age_group_id'] != ID]
        other_ages = other_age_groups['age_group_id'].unique().tolist()

        for age in other_ages:
            age_df = congenital.copy()
            age_df['age_group_id'] = age
            age_df_list.append(age_df)

        congenital = pd.concat(age_df_list)

        return congenital

    def calc_residual(self,
                      etiologies,
                      envelopes,
                      id_cols,
                      val_cols,
                      merge_cols):
        """
        Calculate squeezed estimates for age related and other hearing loss
        by taking the difference of the sum of all etiologies and the
        envelopes. 
        Arguments:
            etiologies (dataframe): Pandas dataframe of estimates representing
                the hearing loss envelopes.
            envelopes (dataframe): Pandas dataframe of estimates representing
                the hearing loss envelopes.
            id_cols (strlist): Dimensions to aggregate by (e.g.
                modelable_entity_id, sex_id and age_group_id).
            val_cols (strlist): The column or columns to sum values for.
            merge_cols (strlist): The column or columns to merge two dataframe
                of draws on for draw level transformations.

        Returns:
            Datafame of estimates for squeezed age related and other hearing
            loss.
        """

        # Sum congenital, otitis and Meningitis
        etiologies = etiologies[etiologies['entity'] != 'oth']
        total_df = aggregate_causes(etiologies, id_cols, val_cols)

        # Assign Age-related and other hearing loss to the difference
        # between the envelope and the sum of congenital, otitis and
        # Meningitis.
        oth = transform_draws(operator_name='sub',
                              df1=envelopes,
                              df2=total_df,
                              merge_cols=merge_cols)
        oth['entity'] = 'oth'
        etiologies = pd.concat([etiologies, oth])

        return etiologies

    def run(self,
            etiologies,
            envelopes,
            squeeze_ages,
            id_cols,
            val_cols,
            merge_cols,
            draw_count):
        """
        Function that acts as a main method and calls methods for each
        step of the etiology squeeze.

        Arguments:
            etiologies (dataframe): Pandas dataframe of estimates
                representing the hearing loss etiologies.
            envelopes (dataframe): Pandas dataframe of estimates
                representing the hearing loss envelopes.
            squeeze_ages (intlist): List of integers representing age
            group_ids that need to be squeezed. All other age groups will
                not be squeezed.
            id_cols (strlist): Dimensions to aggregate by (e.g.
                modelable_entity_id, sex_id and age_group_id).
            val_cols (strlist): The column or columns to sum values for.
            merge_cols (strlist): The column or columns to merge two
                dataframe of draws on for draw level transformations.
            draw_count (int): The number of draws in the dataframe,
                There are usually 1000 draws.

        Returns:
            Estimates for each, etiology, severity and demographic
            dimension that have been squeezed into the respective
            severity specific envelope.
        """

        # 1). Create constant congenital estimates
        et_expand = []
        congenital = self.make_congenital(envelopes=envelopes)
        nondraw_cols = set(congenital.columns) - \
                       set(congenital.filter(like='draw').columns)
        logging.info(nondraw_cols)

        et_expand.append(congenital)
        et_expand.append(etiologies)
        etiologies = pd.concat(et_expand)

        # 2). Squeeze congenital, other, otitis, and all meningitis only up to
        # age 20 
        es_estimates = []
        below_20_et = etiologies.copy()
        below_20_et = below_20_et[below_20_et['age_group_id'].isin(
            squeeze_ages)]
        over_20_et = etiologies.copy()
        over_20_et = over_20_et[~over_20_et['age_group_id'].isin(squeeze_ages)]
        es_estimates.append(over_20_et)

        below_20_envs = envelopes.copy()
        below_20_envs = below_20_envs[below_20_envs['age_group_id'].isin(
            squeeze_ages)]

        below_20 = self.squeeze_etiologies(estimates_to_fit=below_20_envs,
                                           prop_numerator=below_20_et,
                                           prop_denomenator=below_20_et,
                                           id_cols=id_cols,
                                           val_cols=val_cols,
                                           merge_cols=merge_cols,
                                           draw_count=draw_count)
        nondraw_cols = set(below_20.columns) - \
                       set(below_20.filter(like='draw').columns)
        logging.info(nondraw_cols)

        es_estimates.append(below_20)
        etiologies = pd.concat(es_estimates)

        # 3). Stream out congenital over all ages again .
        squeezed_congenital = etiologies.copy()
        no_cong = etiologies.copy()
        squeezed_congenital = squeezed_congenital[
            squeezed_congenital['entity'] == 'cong']
        no_cong = no_cong[no_cong['entity'] != 'cong']

        readj_congenital = self.make_congenital(
            envelopes=squeezed_congenital)

        nondraw_cols = set(readj_congenital.columns) - \
                       set(readj_congenital.filter(like='draw').columns)
        logging.info(nondraw_cols)

        etiologies = pd.concat([readj_congenital, no_cong])

        # 4). For all ages ,
        # assign age-related and other hearing loss as remainder between sum
        # and envelope.
        adj_oth = etiologies.copy()

        squeezed_estimates = self.calc_residual(etiologies=adj_oth,
                                                envelopes=envelopes,
                                                id_cols=id_cols,
                                                val_cols=val_cols,
                                                merge_cols=merge_cols)
        nondraw_cols = set(squeezed_estimates.columns) - \
                       set(squeezed_estimates.filter(like='draw').columns)
        logging.info(nondraw_cols)

        draws_to_correct = squeezed_estimates._get_numeric_data()
        draws_to_correct[draws_to_correct < 0] = 0

        return squeezed_estimates

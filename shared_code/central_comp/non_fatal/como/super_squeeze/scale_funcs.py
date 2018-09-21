import pandas as pd
from transmogrifier import maths

stage_key = {
        'epi': 'epi',
        'id': 'id_noepi',
        'blind': 'blind_noepi_noid'}
drawcols = ['draw_%s' % i for i in range(1000)]


def squeeze_draws(to_squeeze, envelope, scale_up):
    for draw in drawcols:
        if (to_squeeze[draw].sum() > envelope[draw]) or scale_up:
            to_squeeze = maths.scale(
                    to_squeeze,
                    draw,
                    scalar=envelope[draw])
    return to_squeeze


def squeeze_age_group(age_group_id, unsqueezed, env_dict):

    try:
        # Get envelope
        sqzd = unsqueezed[unsqueezed['age_group_id'] == age_group_id]
        sqzd["locked"] = False
        for imp in ['id_bord', 'id_mild', 'id_mod', 'id_sev', 'id_prof', 'epi',
                    'blind']:
            print('Running %s %s' % (age_group_id, imp))

            if imp == 'epi':
                env_frac = 0.95
                scale_up = False
            elif 'id' in imp:
                env_frac = 0.95
                scale_up = False
            else:
                env_frac = 1
                scale_up = True

            # Squeeze to envelope if exceeded
            imp_bin = (sqzd['i_%s' % imp] == 1)
            imp_prev = sqzd[imp_bin]
            other_prev = sqzd[~imp_bin]

            squeezable = imp_prev[~(imp_prev.locked) &
                                  (imp_prev['squeeze'] == "yes")]
            locked = imp_prev[(imp_prev.locked) |
                              (imp_prev['squeeze'] == "no")]

            env = env_dict[imp].copy()
            env = env[env.age_group_id.astype(float).astype(int) ==
                      int(float(age_group_id))]
            env = env[drawcols].squeeze()*env_frac - locked[drawcols].sum()
            env = env.clip(lower=0)
            squeezable = squeeze_draws(squeezable, env, scale_up)
            squeezable["locked"] = True
            sqzd = pd.concat([other_prev, locked, squeezable])

        return sqzd
    except Exception, e:
        return ('error', e)

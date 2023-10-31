
# coding: utf-8

# ## Setup

# In[25]:

'''
Command-line ready GBD Spectrum

Takes demographic and HIV-related inputs and produces
location-, year-, age-, and sex-specific (YAS) estimates of
HIV incidence, prevalence, and mortality.

External inputs (identified with input_folders_*i*.csv)
    1. Epidemic type (concentrated vs. generalized)
    2. Epidemic start year
    3. Region (not used)
    4. Migration (YAS counts)
    5. Baseline population (YAS counts)
    6. Age-specific fertility distribution (YA rates)
        - Normalized to sum to 1.0 on the fly
    7. Total fertility rate (Y rate)
    8. HIV-free mortality (YAS survival probabilities: 1-px)
    9. Sex ratio at birth (Y female births/male births)
    10. HIV mortality without ART (age/CD4 specific)
        - Rates of mortality due to HIV among PLWH not on treatment
    11. HIV mortality with ART
        - Varies by age, sex, CD4 at initiation, and duration on treatment
    12. Adult ART eligibility (Y threshold)
        - CD4 count below which all adults are eligible for ART
    13. HIV fertility ratio (A ratio)
        - Ratio of ASFR in HIV+ women to ASFR in HIV- women
    14. Adult ART coverage (YS count or percentage)
        - Percent or count of eligible adults receiving ART
    15. Progression between CD4 categories (age/CD4 specific rates)
    16. PMTCT coverage (year/treatment specific counts or percentages)
    17. 
'''

import csv, math, sys, logging, time, os, traceback, re, random, getpass
import numpy as np
import pandas as pd
if sys.argv[1]=='-f':
    user = getpass.getuser()
    code_path = '/homes/' + user + '/hiv_gbd2019/decomp_2019/spectrum2019/'
    ISO = 'NLD'
    folder = '180621_numbat_HI'
    run_num = 1
    indiv_run_nums = 1
    stage = 'stage_1'
    group = '2A'
    scaleOnART = 'T'
else:
    code_path =os.path.dirname(os.path.realpath(__file__)) + '/'
    ISO = sys.argv[1]
    folder = sys.argv[2]
    run_num = sys.argv[3]
    indiv_run_nums = int(sys.argv[4])  # Get number of runs this job needs to do
    stage = sys.argv[6]  # Get stage in whole process (pre- or post-incidence adjustment)
    # Check if using alternate incidence directory
    group = sys.argv[7]
    scaleOnART = sys.argv[8]
    if len(sys.argv) > 9:
        if sys.argv[9] != 'none':
            locations['incidence'] = sys.argv[8]

sys.path.append(code_path)
import BeersInterpolation as beers

cohort_groups = ['1B', '2A', '2B']
if stage == 'stage_1' and group in cohort_groups:
    inc_adj_config = 1
    cohort_output = True
else:
    inc_adj_config = 1
    cohort_output = False

# Toggles
detailed_output = True
art_deaths_output = True
usePredCoverage = True
maxYear = 2019


if scaleOnART == 'T':
    scaleOnART = True
else:
    scaleOnART = False

# Paths
directory = '/home/j/Project/Causes of Death/codem/models/A02/GBD 2013 HIV/Estimates/' + folder
demProjFilePath = "/ihme/hiv/spectrum_input/" + folder + "/"
AIMfilepath = "/home/j/WORK/04_epi/01_database/02_data/hiv/04_models/gbd2015/02_inputs/AIM_assumptions/"

# ## Model constants and dictionaries
# Get parent country (ISO) if needed
if ISO.find('_') != -1:
    parent = ISO.split('_')[0]
else:
    parent = ISO

# In[ ]:

# Fixed parameters
minAge = 0
maxAge = 80
output_type = 'five_year'

# Get first year of projection
if 'MOZ' not in ISO:
    minYear = 1970
else:
    minYear = 1982
if ISO == 'KHM':
    minYear = 1980          
if 'IND' in ISO:
    minYear = 1981
if 'MDA' in ISO:
    minYear = 1981          
if 'CHN' in ISO:
    minYear = 1980
if 'ZAF' in ISO:
    minYear = 1985
if ISO in ['TWN', 'PRK']:
    minYear = 1980
    
sexes = ['male','female']
sex_nums = {'male': 1, 'female': 2}
noARTCD4states = ['LT50CD4', '50to99CD4', '100to199CD4', '200to249CD4',
'250to349CD4', '350to500CD4', 'GT500CD4']
# Create a copy of noARTCD4states with 'neg' included for interation convenience
noARTCD4statesNeg = ['neg', 'LT50CD4', '50to99CD4', '100to199CD4', '200to249CD4',
'250to349CD4', '350to500CD4', 'GT500CD4']
ARTCD4states = ['ARTLT50CD4', 'ART50to99CD4', 'ART100to199CD4', 'ART200to249CD4',
'ART250to349CD4', 'ART350to500CD4', 'ARTGT500CD4']
CD4Caps = {'LT50CD4':1, '50to99CD4':1, '100to199CD4':1, '200to249CD4':1,'250to349CD4':.9, '350to500CD4':.9, 'GT500CD4':.78}
# All positive HIV states
allCD4states = list(noARTCD4states)
allCD4states.extend(ARTCD4states)
# All HIV states excluding 'all'
# (Useful for iteration)
allCD4statesNeg = list(allCD4states)
allCD4statesNeg.append('neg')

noARTChildHIVStates = ['perinatal', 'BF0', 'BF7',"BF12"]
u5CD4States = ["GT30", "26to30", "21to25", "16to20", "11to15", "5to10", "LT5"]
u15CD4States = ["GT1000CD4", "750to999CD4","500to749CD4", "350to499CD4", "200to349CD4", "LT200CD4"]
adultARTdurations = ['LT6Mo', '6to12Mo', 'GT12Mo']
PMTCTtreatmentOptions = ['singleDoseNevir', 'dualARV', 'optionA', 'optionB',
'tripleARTbefPreg', 'tripleARTdurPreg']
postnatalProphOptions = ['optionA', 'optionB']
allInterventions = ['ART', 'CTX', 'singleDoseNevir', 'dualARV', 'tripleARTdurPreg', 'tripleARTbefPreg','optionA', 'optionB', 'optionA_BF', 'optionB_BF']
minDuration = 1
maxAdultDuration = 30
maxChildDuration = 15
childMaxAge = 14

mortalityAgeCategories = ["15-24", "25-34", "35-44", "45-54", "55-99"]

possibleCD4categories = ['LT200', '200to350', '350to500', '500to750', '750to1000',
'1000to1500', '1500to2000', '2000+']
possibleCD4categoryCountValues = [200, 350, 500, 750, 1000, 1500, 2000, 10000]
possibleCD4categoryPercentValues = [5, 10, 15, 20, 25, 30, 40, 100]
possibleCD4categoryCounts = {}
possibleCD4categoryPercents = {}
for c in xrange(len(possibleCD4categories)):
    possibleCD4categoryCounts[possibleCD4categories[c]] = possibleCD4categoryCountValues[c]
    possibleCD4categoryPercents[possibleCD4categories[c]] = possibleCD4categoryPercentValues[c]


csvData = []
coverageData = []
appended_cov_data = []
appended_deaths_data = []
appended_art_deaths_data = []

cohortData = []

var_names = ['run_num', 'year', 'age', 'sex']
death_vars = ['d_' + str(i) for i in xrange(minYear, maxYear+1)]
var_names.extend(death_vars)
cohortData.append(var_names)

# ## Functions

# In[26]:

def flattenYearBySex(obj, year, sex, status = None, duration = None):
    ''' Takes population-like object, a year within
the range of the projection, and sex (which can
be "total" for the births object), and returns
those values for that sex in that year for each
individual age as a list.'''
    temp = []
    if not status and not duration:
        for i in obj:
            temp.append(obj[i][year][sex])
    elif status:
        for i in obj:
            temp.append(obj[i][year][sex][status])
        if duration:
            for i in obj:
                temp.append(obj[i][year][sex][status][duration])
    return temp

def totalByYear(obj, year, sex, status = None, duration = None):
    ''' Sums the list returned by flattenYearBySex
to give yearly, sex-specific totals.'''
    return np.sum(flattenYearBySex(obj, year, sex, status, duration))

def monthConcordance(age):
    if age == 0:
        return 0
    elif age in [1,2]:
        return 1
    elif age in [3,4]:
        return 2
    else:
        return 3

def normBounds(a, b):
    '''Get random variable from truncated normal distribution between a and b inclusive.'''
    val = 100
    while not(val >= a and val <= b):
        val = np.random.normal(0, 100)
    return val

def updateAllStateTotal(year):
    '''Update "all" category in population object.'''
    for age in population:
        for sex in sexes:
            population[age][year][sex]['all'] = 0.0
            if age > 15:
                for c in noARTCD4statesNeg:
                    if c == 'neg':
                        population[age][year][sex]['all'] += population[age][year][sex][c]
                    else:
                        population[age][year][sex]['all'] += sum(population[age][year][sex][c])
                for c in ARTCD4states:
                    for d in population[age][year][sex][c]:
                        population[age][year][sex]['all'] += sum(population[age][year][sex][c][d])
            elif age <= 4:
                population[age][year][sex]['all'] += population[age][year][sex]['neg']
                for c in u5CD4States:
                    for d in noARTChildHIVStates:
                        population[age][year][sex]['all'] += sum(population[age][year][sex][c][d])
                    for d in adultARTdurations:
                        population[age][year][sex]['all'] += sum(population[age][year][sex][c][d])

            elif age >=5 and age <15:
                population[age][year][sex]['all'] += population[age][year][sex]['neg']
                for c in u15CD4States:
                    for d in noARTChildHIVStates:
                        population[age][year][sex]['all'] += sum(population[age][year][sex][c][d])
                    for d in adultARTdurations:
                        population[age][year][sex]['all'] += sum(population[age][year][sex][c][d])



def getEligiblePregnantWomen(age, t):
    age5 = age - age % 5
    PW = 0
    age5index = (age - age % 5) / 5 - 3
    numEligible = 0
    numIneligible = 0
    for c in noARTCD4states:
        PW += sum(population[age][t]['female'][c])
    PW = PW * TFR[t] * ASFRbyAge[age5][t-minYear]/np.sum(ASFRbyYear[t]) / 5 * TFRreduction[age5index]
    for c in noARTCD4states:
        if CD4lowerLimits[c] <= adultARTeligibility[t-minYear]:
            numEligible += sum(population[age][t]['female'][c])
        else:
            numIneligible += sum(population[age][t]['female'][c])
    if numEligible + numIneligible > 0:
        pregWomenNeed = PW * numEligible / (numEligible + numIneligible)
    else:
        pregWomenNeed = 0
    return pregWomenNeed

#TF - updated with child update August 2017
def getARTpatients(a1, a2, t, sex):
    tempSum = 0
    for age in xrange(a1, a2+1):
        if age > childMaxAge:
            for c in ARTCD4states:
                for d in adultARTdurations:
                    tempSum += sum(population[age][t][sex][c][d])
        else:
            if age < 5:           
                for c in u5CD4States:
                    tempSum += sum([sum(population[age][t][sex][c][d]) for d in adultARTdurations])
            else:
                for c in u15CD4States:
                    tempSum += sum([sum(population[age][t][sex][c][d]) for d in adultARTdurations])
                
    return tempSum

def calcBFtransmission(m1, m2, t):
    BFTR = 0
    percentOptA = treatPercent['optionA_BF']
    percentOptB = treatPercent['optionB_BF']

    dropoutOptA = postnatalDropout['optionA'][t-minYear]
    dropoutOptB = postnatalDropout['optionB'][t-minYear]

    optionATransRate = MTCtransRates['optionA']['BFGE350']
    optionBTransRate = MTCtransRates['optionB']['BFGE350']

    if propGE350 > 0:
        if (percentOptA + percentOptB - treatPercent['tripleARTbefPreg'] - treatPercent['tripleARTdurPreg']) > propGE350:
            excess = (percentOptA + percentOptB - treatPercent['tripleARTbefPreg'] - treatPercent['tripleARTdurPreg']) - propGE350
            optionATransRate = (propGE350 * MTCtransRates['optionA']['BFGE350'] + excess * 1.45 / 0.46 * MTCtransRates['optionA']['BFGE350']) / (propGE350 + excess)
            optionBTransRate = (propGE350 * MTCtransRates['optionB']['BFGE350'] + excess * 1.45 / 0.46 * MTCtransRates['optionB']['BFGE350']) / (propGE350 + excess)
    for d in xrange(m1, m2+1):
        percentOptA = treatPercent['optionA'] / (math.e ** (d * 2 * math.log(1 + dropoutOptA / 100)))
        percentOptB = treatPercent['optionB'] / (math.e ** (d * 2 * math.log(1 + dropoutOptB / 100)))
        percentNoProph = 1 - percentOptA - percentOptB - treatPercent['tripleARTbefPreg'] - treatPercent['tripleARTdurPreg']
        if percentNoProph < 0:
            percentNoProph = 0
        BFTR += (((1 - percentBFnoART[t-minYear][d] / 100) * (1 - percentInProgram)
            + (1 - percentBFonART[t-minYear][d] / 100) * percentInProgram)
            * percentNoProph
            * (propLT350 * MTCtransRates['LT200']['BFLT350']
                + propGE350 * MTCtransRates['GT350']['BFGE350']
                + propIncidentInfections / 12 * MTCtransRates['IncidentInf']['BFLT350']))
        BFTR += (1 - percentBFonART[t-minYear][d] / 100) * percentOptA * optionATransRate
        BFTR += (1 - percentBFonART[t-minYear][d] / 100) * percentOptB * optionBTransRate

        if getARTpatients(15, maxAge, t, 'female') <= 0:
            propNewART = 0
        else:
            propNewART = (currentYearART['female'] - prevYearART['female']) / getARTpatients(15, maxAge, t, 'female')
        BFTR += ((1 - percentBFonART[t-minYear][d] / 100) * treatPercent['tripleARTbefPreg']
            * ((1 - propNewART) * MTCtransRates['tripleARTbefPreg']['BFLT350']
                + propNewART * MTCtransRates['tripleARTdurPreg']['BFLT350']))
        BFTR += ((1 - percentBFonART[t-minYear][d] / 100) * treatPercent['tripleARTdurPreg']
            * (propNewART * MTCtransRates['tripleARTbefPreg']['BFLT350']
                + (1 - propNewART) * MTCtransRates['tripleARTdurPreg']['BFLT350']))

    return BFTR * 2

def getBirths(t, sex = None):
    tempSum = 0
    for age in xrange(15, 50):
        if sex:
            tempSum += births[age][t][sex]
        else:
            tempSum += births[age][t]['total']
    return tempSum

#New functions to make it possible to use predicted ART coverage
#Predicted ART coverage provided by the forecasting team as of Jan 2017

def getARTSurvivors(adultARTdurations,onARTmortality,sex,t,timeStep, population, age, cd4, onARTScalar):
    ARTsurvivors = 0
    age10 = ((age - (age - 5) % 10) - 15) / 10
    if age > 55:
        age10 = ((55 - (55 -5) % 10) - 15) / 10
    for d in adultARTdurations:
        alpha = onARTmortality[sex][d][age10]["ART" +cd4] * onARTScalar
        ARTsurvivors += sum(population[age][t][sex]["ART" +cd4][d]) * (1 - alpha / timeStep)
    return ARTsurvivors

#scaling so the input predicted coverage will add to the input coverage counts
def getPredARTCoverageCounts(popByCD4, predARTCoverage, t, currentYearART, noARTCD4states):
    ARTCoverageCounts = {'female':{}, 'male':{}}
    over100PercentCovered = False
    for sex in ARTCoverageCounts.keys():
        # Add up ages and cd4 count to get sex specific
        denom = sum([sum([popByCD4[sex][age][c] * predARTCoverage[t][age][sex][c] for c in noARTCD4states]) for age in range(15,81)])
        for age in range(15,81): 
            ARTCoverageCounts[sex][age] = {}
            if denom == 0:
                for c in noARTCD4states:
                    ARTCoverageCounts[sex][age][c] = 0
            else:  
                scalar = currentYearART[sex]/denom
                for c in noARTCD4states:
                    percentCovered = scalar * predARTCoverage[t][age][sex][c]
                    if percentCovered > CD4Caps[c]:
                        predARTCoverage[t][age][sex][c] = CD4Caps[c]
                        percentCovered = CD4Caps[c]
                        over100PercentCovered = True
                    ARTCoverageCounts[sex][age][c] = percentCovered * popByCD4[sex][age][c]
        if over100PercentCovered == True:
            ARTCoverageCounts = rescale(popByCD4, predARTCoverage, sex, t, currentYearART, ARTCoverageCounts)
    return ARTCoverageCounts

def rescale(popByCD4, predARTCoverage, sex, t, currentYearART, ARTCoverageCounts):  
    #recurse flag will be true if, after rescaling, we create more percentCovered > 1
    recurse = True
    while recurse == True:
        recurse = False
        #if all coverages are equal to 1, stop looping
        flag = True
        for age in range(15, 81):
            for c in noARTCD4states:
                if predARTCoverage[t][age][sex][c] < 1:
                    flag = False
        if flag == True:
            print "base case"
            return ARTCoverageCounts
        total = currentYearART[sex]
        #subtracting groups with percent covered == 1 from the total
        for age in range(15, 81):
            total -= sum([popByCD4[sex][age][c] for c in noARTCD4states if predARTCoverage[t][age][sex][c] >= 1])
        denom =  sum([sum([popByCD4[sex][age][c] * predARTCoverage[t][age][sex][c] for c in noARTCD4states if predARTCoverage[t][age][sex][c] < 1]) for age in range(15, 81)])
        scalar = total/denom
        #rescale to groups where percent covered != 1
        for age in range(15,81):
            for c in noARTCD4states:
                if predARTCoverage[t][age][sex][c] < 1:
                    percentCovered = scalar * predARTCoverage[t][age][sex][c]
                    if percentCovered > 1:
                        recurse = True
                        print "recurse"
                        percentCovered = 1
                        predARTCoverage[t][age][sex][c] = 1
                    ARTCoverageCounts[sex][age][c] = percentCovered * popByCD4[sex][age][c]
    return ARTCoverageCounts

#Used for calculation #people living with HIV by CD4 counts for pred ART coverage counts
def getPopByCD4(population, noARTCD4states):
    popByCD4 = {'female':{}, 'male':{}}
    for sex in popByCD4.keys():
        for age in range(15, 81):
            popByCD4[sex][age] = {}
            for c in noARTCD4states:             
                popNoART = sum(population[age][t][sex][c])
                popART = 0
                for d in adultARTdurations:
                    popART += sum(population[age][t][sex]["ART" + c][d])
                popByCD4[sex][age][c] = (popNoART + popART)
    return popByCD4

#Reading in predicted ART coverage (Feb 2019)
def readPredARTCoverage(ISO):
    import csv
    caps_path = "/home/j/WORK/04_epi/01_database/02_data/hiv/04_models/gbd2015/02_inputs/coverage_caps/190124_decomp19/" + ISO + ".csv"
    with open(caps_path, 'r') as f:
        f_reader = csv.reader(f)
        caps_data = [row for row in f_reader]
    predARTIndex = caps_data[0].index('cov_pred')
    predARTSexIndex = caps_data[0].index('sex')
    predARTAgeIndex = caps_data[0].index('age')
    predARTYearIndex = caps_data[0].index('year')
    predARTcd4Index = caps_data[0].index('CD4_cat')
    cd4Values = set([int(row[predARTcd4Index]) for row in caps_data[1:]])
    predARTCoverage = {}
    predARTCoverageRatio = {}
    for year in range(minYear,maxYear + 1):
        predARTCoverage[year] = {}
        predARTCoverageRatio[year] = {}
        for age in range(15,81):
            predARTCoverage[year][age] = {}
            predARTCoverage[year][age]['male'] = {x:0 for x in noARTCD4states}
            predARTCoverage[year][age]['female'] ={x:0 for x in noARTCD4states}
            predARTCoverageRatio[year][age] = {}
            predARTCoverageRatio[year][age]['male'] = {x:1 for x in noARTCD4states}
            predARTCoverageRatio[year][age]['female'] ={x:1 for x in noARTCD4states}
    for row in caps_data[1:]:
        predARTAge = int(row[predARTAgeIndex])
        predARTYear = int(row[predARTYearIndex])
        predARTSex = int(row[predARTSexIndex])
        predARTcd4 = int(row[predARTcd4Index])
        if predARTcd4 == 0:
            c = "LT50CD4"
        elif predARTcd4 == 500:
            c = "GT500CD4"
        elif predARTcd4 == 50:
            c = "50to99CD4"
        else:
            c = [x for x in noARTCD4states if x[0:3] == str(predARTcd4)][0]
        if predARTYear in predARTCoverage.keys():
            if predARTAge in predARTCoverage[predARTYear].keys():
                if predARTSex == 1:
                    predARTCoverage[predARTYear][predARTAge]['male'][c] = float(row[predARTIndex])
                else:
                    predARTCoverage[predARTYear][predARTAge]['female'][c] = float(row[predARTIndex])
    if group[0] == '2':
        predARTCoverage = rescalePredCoverageByEligibility(predARTCoverage)                    
    return predARTCoverage
## For locations where input coverage is a percent, we still use eligible groups to define the population needing ART
## We rescale the predicted ART coverage proportions after removing non-eligibile groups
def rescalePredCoverageByEligibility(predARTCoverage):
    for t in range(minYear, maxYear + 1):
        for sex in sexes:
            for a in range(15, 81):
                ## Set ineligible groups to 0
                for c in noARTCD4states:
                    if c == 'LT50CD4':
                        lowerEligGroup = 'LT50CD4'
                    else:
                        lowerEligGroup = noARTCD4states[noARTCD4states.index(c) -1]
                    if CD4lowerLimits[lowerEligGroup] > adultARTeligibility[t-minYear]:
                        predARTCoverage[t][a][sex][c] = 0.01
    return predARTCoverage


def findU15EligibilityGroups(age, t, childEligibilityCount, u15CD4States):
    age1 = monthConcordance(age)
    if childEligibilityCount[age1][t-minYear] >= 999:
        CD4Elig = u15CD4States
    elif childEligibilityCount[age1][t-minYear] <= 348:
            CD4Elig = u15CD4States[5:]
    else:
        for c in range(1,5):
            if int(u15CD4States[c][5:8]) <= childEligibilityCount[age1][t-minYear]:
                CD4Elig = u15CD4States[c:]
                break
    return CD4Elig      

def findU5EligibilityGroups(age, t, childEligibilityPercent, u5CD4States):
    age1 = monthConcordance(age)
    if childEligibilityPercent[age1][t-minYear] >= 30:
        CD4Elig = u5CD4States
    elif childEligibilityPercent[age1][t-minYear] <= 9:
        CD4Elig = u5CD4States[6:]
    else:
        for c in range(1,6):
            if int(u5CD4States[c][-2:]) <= childEligibilityPercent[age1][t-minYear]:
                CD4Elig = u5CD4States[c:]
                break
    return CD4Elig 

col = lambda data, str: data[0].index(str)


# ## Read Inputs

# In[27]:

# Epidemic Type
inputEpidemicTypeData = beers.openCSV(AIMfilepath + 'classification/epi_class/default_EPP_combo/epi_class_2019a.csv')
epidemicType = [row[1] for row in inputEpidemicTypeData if row[0] == ISO][0]

if epidemicType == 'Custom':
    epidemicType = 'CON'
if parent == 'IND':
    epidemicType = 'IND'

# Epidemic Start Year
try:
    inputEpidemicStartYearData = beers.openCSV('/home/j/Project/Causes of Death/codem/models/A02/GBD 2013 HIV/Program_inputs/data/AIM_assumptions/classification/epi_start_yr/defaults/epi_start_year.csv')
    epidemicStartYear = [int(row[1]) for row in inputEpidemicStartYearData if row[0] == ISO][0]
except:
    epidemicStartYear = 1984

## Demographic Inputs
# Migration
inputMigrationData = beers.openCSV(demProjFilePath + "/migration/" + ISO + ".csv")
# Population
inputPopData = beers.openCSV(demProjFilePath + "/population/" + ISO + ".csv")
# Total fertility rate (TFR)
inputTFRdata = beers.openCSV(demProjFilePath +  "/TFR/" + ISO + ".csv")
# Age-specific fertility rate (ASFR)
inputASFRdata = beers.openCSV(demProjFilePath + "/ASFR/" + ISO + ".csv")
# HIV-free mortality
inputSRdata = beers.openCSV('/ihme/gbd/WORK/02_mortality/03_models/5_lifetables/results/hivfree_sx/locs/' + ISO + "_life_tables.csv")
# Sex ratio at birth (SRB)
inputSRBdata = beers.openCSV(demProjFilePath  + "/SRB/" + ISO + ".csv")

## Transition parameters
# Off-ART Mortality
inputNoARTMortality = beers.openCSV(demProjFilePath + '/noARTmortality/' + ISO + ".csv")
# On-ART Mortality
inputOnARTMortality = beers.openCSV(demProjFilePath + "/onARTmortality/" + ISO + ".csv")
# CD4 progression
inputProgressionParameters = beers.openCSV(demProjFilePath + "/averageCD4duration/" + ISO + ".csv")

# On-ART Mortality scalar
inputOnARTScalar = pd.read_csv("/home/j/WORK/04_epi/01_database/02_data/hiv/04_models/gbd2015/02_inputs/artcc_rr.csv")

# Child Cotrimoxazole effect
inputChildCTXeffect = beers.openCSV(AIMfilepath + "/transition_parameters/Child_treatment_effects_cotrim.csv")

## Treatment parameters
# Adult ART Eligibility
inputAdultARTeligibility = beers.openCSV(demProjFilePath +  '/adultARTeligibility/' + ISO + ".csv")
# Adult ART Coverage
inputAdultARTCoverage = beers.openCSV(demProjFilePath + '/adultARTcoverage/' + ISO + ".csv")
# Child ART Eligibility
inputChildARTelgibility = beers.openCSV(demProjFilePath + "/childARTeligibility/" + ISO + ".csv")
# Child ART Coverage
inputChildARTCoverage = beers.openCSV(demProjFilePath + '/childARTcoverage/' + ISO + '.csv')
# Prevention of Mother to Child Transmission (PMTCT)
inputPMTCT = beers.openCSV(demProjFilePath + '/PMTCT/' + ISO + '.csv')
# TFR reduction
inputTFRreduction = beers.openCSV(demProjFilePath + "/TFRreduction/" + ISO + ".csv")
# Postnatal Dropout
inputPostnatalDropout = beers.openCSV(demProjFilePath + '/PMTCTdropoutRates/' + ISO + '.csv')
# Percent Breastfeeding
inputPercentBF = beers.openCSV(demProjFilePath + '/percentBF/' + ISO + '.csv')
# Special eligibility populations
inputEligiblePops = beers.openCSV(AIMfilepath + "/treatment_eligibility/pops_eligible_regardless/pops_eligible_regardless_cd4_UNAIDS_2016.csv")

## Incidence and Prevalence    
# Incidence Hazard Percent
inputEPPinc = beers.openCSV(demProjFilePath + "/incidence/"  + ISO + ".csv")
if stage == 'stage_2':
    inputAdjInc = beers.openCSV("/ihme/hiv/ciba_temp/" + folder + "_adj" + '/' + ISO + "_SPU_inc_draws.csv")
    
## Feb 2019 - reading in GBD16 final numbers for stage 1 incidence input for group 2
if group[0] == '2' and stage == 'stage_1':
    inputAdjInc = beers.openCSV('/ihme/hiv/spectrum_input/GBD19_stage1_priors_final/' + ISO + '.csv')
#     
# Prevalence Rate Percent
inputEPPprev = beers.openCSV(demProjFilePath + "/prevalence/"  + ISO + ".csv")
# Sex ratio of incidence (by epidemic type)

if group[0] == '1':
    if parent != 'IND':
        inputIncSexRatio = beers.openCSV("/ihme/hiv/spectrum_input/FtoM_inc_ratio/" + ISO[:3] + '.csv')
    else:
        inputIncSexRatio = beers.openCSV("/ihme/hiv/spectrum_input/FtoM_inc_ratio/" + ISO + '.csv')
    sexRatioAdj = 1
else:
    inputIncSexRatio = beers.openCSV(AIMfilepath + '/sex_age_pattern/FtoM_inc_ratio_epidemic_specific.csv')
    # We need to adjust the sex ratio of incidence in order to get the sex ratio of
    # deaths to line up with the ratio in the VR data.
    try:
        inputSexRatioAdj = beers.openCSV(AIMfilepath + "/sex_age_pattern/post_1995_sex_ratios.csv")
        
        isoIndex = inputSexRatioAdj[0].index('iso3')
        sexRatioVR = float([row for row in inputSexRatioAdj[1:] if row[isoIndex] == parent][0][1])

        sexRatioAdj = sexRatioVR / .42
    except:
        sexRatioAdj = 1
# Age distribution of incidence
inputIncAgeDist = beers.openCSV(AIMfilepath + "sex_age_pattern/age_IRRs/Feb17/GEN_IRR.csv")
# CD4 distrubtion at infection
inputInitCD4dist = beers.openCSV(AIMfilepath + "transition_parameters/CD4dis_infection/CD4count_distribution_at_infection.csv")
# Mother to Child Transmission Rates

inputMTCtransRates= beers.openCSV(AIMfilepath + "program_stats/PMTCT_transmission_rts_2016.csv")
# Child CD4 distrubtion at infection for counts of eligibility
inputChildCD4countDist = beers.openCSV(AIMfilepath + 'transition_parameters/childCD4countDist.csv')
# Child CD4 distrubtion at infection for percent eligible
inputChildCD4percentDist = beers.openCSV(AIMfilepath + 'transition_parameters/childCD4percentDist.csv')


childHIVMortalityOffART = pd.read_csv(demProjFilePath + "/childMortNoART/" + ISO + ".csv", index_col = False)
childHIVMortalityOnART = pd.read_csv(demProjFilePath + "/childMortOnART/" + ISO + ".csv", index_col = False)
childProgressionParameters = pd.read_csv(demProjFilePath + "/childProgParam/" + ISO + ".csv", index_col = False)
CD4PercAtInf = pd.read_csv(demProjFilePath + "/childDistNewInf/" + ISO + ".csv", index_col = False)
childARTDist = pd.read_csv(demProjFilePath + "/childARTDist/" + ISO + ".csv", index_col = False)


## Seeds
seed_dir = '140825'
seed_loc = ISO
seed_path = '%sseeds/%s/%s_seeds.csv' % ("/home/j/WORK/04_epi/01_database/02_data/hiv/04_models/gbd2015/02_inputs/", seed_dir, seed_loc)
try:
    with open(seed_path, 'r') as f:
        f_reader = csv.reader(f)
        seed_data = [row for row in f_reader]
except:
    seed_path = '%sseeds/%s/%s_seeds.csv' % ("/home/j/WORK/04_epi/01_database/02_data/hiv/04_models/gbd2015/02_inputs/", seed_dir, "DEF")
    with open(seed_path, 'r') as f:
        f_reader = csv.reader(f)
        seed_data = [row for row in f_reader]
run_i = seed_data[0].index('run')
seed_i = seed_data[0].index('seed')
seeds = {int(row[run_i]): int(float(row[seed_i])) for row in seed_data[1:]}

## Additional object prep
# Prepare migration object
# (Allows for varying variable positions)
yearIndexMig = inputMigrationData[0].index('year')
sexIndexMig = inputMigrationData[0].index('sex')
ageIndexMig = inputMigrationData[0].index('age')
valueIndexMig = inputMigrationData[0].index('value')

# Restrict data to year range
inputMigrationData = [[val for val in row] for row in inputMigrationData[1:] if int(row[yearIndexMig]) <= maxYear]

data_arr = np.array(inputMigrationData, dtype=float)

sorted_arr = data_arr[np.lexsort((data_arr[:,ageIndexMig], data_arr[:,sexIndexMig], data_arr[:, yearIndexMig]))]
inputMigrationData = [list(arr) for arr in sorted_arr]

# Population Indexes
yearIndexPop = inputPopData[0].index('year')
sexIndexPop = inputPopData[0].index('sex')
ageIndexPop = inputPopData[0].index('age')
valueIndexPop = inputPopData[0].index('value')  

# Restrict to base year
inputPopData = [[val for val in row] for row in inputPopData[1:] if int(row[yearIndexPop]) == minYear]

# TFR index
# Get variable locations
yearIndexTFR = inputTFRdata[0].index('year')
valueIndexTFR = inputTFRdata[0].index('value')

# Flatten array and restrict to year range
inputTFRdata = [[val for val in row] for row in inputTFRdata[1:] if int(row[yearIndexTFR]) <= maxYear]

data_arr = np.array(inputTFRdata, dtype=float)

sorted_arr = data_arr[data_arr[:, yearIndexTFR].argsort()]
inputTFRdata = [list(arr) for arr in sorted_arr]

# Survival Rates index
yearIndexSurv = inputSRdata[0].index('year')
sexIndexSurv = inputSRdata[0].index('sex')
ageIndexSurv = inputSRdata[0].index('age')

surv_vars = inputSRdata[0]

inputSRdata = [[val for val in row] for row in inputSRdata[1:] if float(row[yearIndexSurv]) <= maxYear]

for i in xrange(len(inputSRdata)):
    if int(inputSRdata[i][ageIndexSurv]) == 80 & int(inputSRdata[i-1][ageIndexSurv]) == 80:
        inputSRdata[i][ageIndexSurv] = 81
        
# SRBIndex
# Get variable locations
yearIndexSRB = inputSRBdata[0].index('year')
valueIndexSRB = inputSRBdata[0].index('value')

# Flatten array and restrict data to year range.
inputSRBdata = [[val for val in row] for row in inputSRBdata[1:] if int(row[yearIndexSRB]) <= maxYear]

# ASFR Prep
# Get locations of variables
yearIndex = inputASFRdata[0].index('year')
ageIndex = inputASFRdata[0].index('age')
# if 'IND' not in ISO:
valueIndex = inputASFRdata[0].index('value')
# else:
#   valueIndex = inputASFRdata[0].index('v')

# Restrict data to year range and five-year age groups
inputASFRdata = [[val for val in row] for row in inputASFRdata[1:] if int(row[yearIndex]) >= minYear and int(row[yearIndex]) <= maxYear and int(row[ageIndex]) % 5 == 0]

# Multiply by the appropriate factor to get to five-year percentages
# if 'spectrum_demproj' in locations['ASFR'] :
    # The default data need to be re-aggregated into five-year age groups
# factor = 500
# else:
#   # The GBD data do not need to be re-aggregated.
factor = 100

# Create and fill ASFR objects
# "byYear" is only used for easy summation
ASFRbyAge = {}
for age in xrange(15, 50, 5):
    ASFRbyAge[age] = [float(row[valueIndex]) * factor for row in inputASFRdata if int(row[ageIndex]) == age]
ASFRbyYear = {}
for year in xrange(minYear, maxYear + 1):
    ASFRbyYear[year] = [float(row[valueIndex]) * factor for row in inputASFRdata if int(row[yearIndex]) == year]

# Adult ART Coverage
yearIndex = inputAdultARTCoverage[0].index('year')
sexIndex = inputAdultARTCoverage[0].index('sex')
numIndex = inputAdultARTCoverage[0].index('ART_cov_num')
pctIndex = inputAdultARTCoverage[0].index('ART_cov_pct')

data_arr = np.array(inputAdultARTCoverage[1:], dtype=float)
sorted_arr = data_arr[np.lexsort((data_arr[:,yearIndex], data_arr[:,sexIndex]))]

tmpAdultARTCoverage = [list(arr) for arr in sorted_arr]
tmpAdultARTCoverage.insert(0, inputAdultARTCoverage[0])

inputAdultARTCoverage = tmpAdultARTCoverage


# PMTCT
yearIndexPMTCT = inputPMTCT[0].index('year')

data_arr = np.array(inputPMTCT[1:], dtype=float)
sorted_arr = data_arr[data_arr[:,yearIndexPMTCT].argsort()]

tmpPMTCTcoverage = [list(arr) for arr in sorted_arr]
tmpPMTCTcoverage.insert(0, inputPMTCT[0])

inputPMTCT = tmpPMTCTcoverage

#putting everything from the pandas into dictionaries
childHIVMort = {}
for age in range(0,15):
    childHIVMort[age] = {}
    if age < 5:
        CD4States = u5CD4States
    else:
        CD4States = u15CD4States
    for c in CD4States:
        childHIVMort[age][c] = {}
        for sex in sexes:
            childHIVMort[age][c][sex] = {}
            for d in noARTChildHIVStates:
                childHIVMort[age][c][sex][d] = childHIVMortalityOffART.loc[(childHIVMortalityOffART['age'] == age) & (childHIVMortalityOffART['birth_category'] == d) & (childHIVMortalityOffART['CD4'] == c)].value.item()
            for d in adultARTdurations:
                childHIVMort[age][c][sex][d] = childHIVMortalityOnART.loc[(childHIVMortalityOnART['sex'] == sex) & (childHIVMortalityOnART['age'] == age) & (childHIVMortalityOnART['category'] == d) & (childHIVMortalityOnART['CD4'] == c)].value.item()
childProgParam = {}
for age in range(0,15):
    childProgParam[age] = {}
    if age < 5:
        CD4States = u5CD4States
    else:
        CD4States = u15CD4States
    for sex in sexes:
        childProgParam[age][sex] = {}
        for c in CD4States:
            if c != "LT5" and c!= "LT200CD4":
                childProgParam[age][sex][c] = childProgressionParameters.loc[(childProgressionParameters['age'] == age) & (childProgressionParameters['CD4'] == c) & (childProgressionParameters['sex'] == sex) ].value.item()
CD4PercAtInfection = {}
for c in u5CD4States:
    CD4PercAtInfection[c] = float(CD4PercAtInf.loc[(CD4PercAtInf['CD4'] == c)].value.item() )/100
childARTDistribution = {}
for age in range(0,15):
    childARTDistribution[age] = {}
    for t in range(minYear, maxYear + 1):
        childARTDistribution[age][t] = childARTDist.loc[(childARTDist['age'] == age) & (childARTDist['year'] == t)].value.item()

    # run_id = 1
for run_id in xrange(indiv_run_nums):
    iteration_number = 0  # Maybe take this out
    individual_id = (int(run_num) - 1) * indiv_run_nums + run_id + 1

    # Set seeds
    np.random.seed(seeds[individual_id])
    random.seed(seeds[individual_id])

    # Initialize key objects
    adjustments = {}
    population = {}
    deaths = {}
    AIDSdeaths = {}
    needForART = {}

    # Initialize storage for population and death objects
    for obj in [population, deaths, AIDSdeaths, needForART]:
        for i in xrange(minAge, maxAge + 1):
            obj[i] = {}
            for j in xrange(minYear, maxYear + 1):
                obj[i][j] = {}
                for k in sexes:
                    if obj == population:
                        obj[i][j][k] = {}
                        if i > childMaxAge:
                            for l in noARTCD4statesNeg:
                                obj[i][j][k][l] = []
                                for m in xrange(minYear, maxYear + 1):
                                    obj[i][j][k][l].append(0.0)
                            for l in ARTCD4states:
                                obj[i][j][k][l] = {}
                                for d in adultARTdurations:
                                    obj[i][j][k][l][d] = []
                                    for m in xrange(minYear, maxYear + 1):
                                        obj[i][j][k][l][d].append(0.0)

                        #U5 CD4 states divided by percent
                        elif i <=4:
                            obj[i][j][k]["neg"] = 0
                            obj[i][j][k]['all'] = 0
                            for c in u5CD4States:
                                obj[i][j][k][c] = {}
                                # Not on ART
                                for d in noARTChildHIVStates:
                                    obj[i][j][k][c][d] = []
                                    for m in xrange(minYear, maxYear + 1):
                                        obj[i][j][k][c][d].append(0.0)
                                #On ART
                                for d in adultARTdurations:
                                    obj[i][j][k][c][d] = []
                                    for m in xrange(minYear, maxYear + 1):
                                        obj[i][j][k][c][d].append(0.0)
                        #5-14 CD4 counts
                        else:
                            obj[i][j][k]["neg"] = 0
                            obj[i][j][k]['all'] = 0
                            for c in u15CD4States:
                                obj[i][j][k][c] = {}
                                # Not on ART
                                for d in noARTChildHIVStates:
                                    obj[i][j][k][c][d] = []
                                    for m in xrange(minYear, maxYear + 1):
                                        obj[i][j][k][c][d].append(0.0)
                                #On ART
                                for d in adultARTdurations:
                                    obj[i][j][k][c][d] = []
                                    for m in xrange(minYear, maxYear + 1):
                                        obj[i][j][k][c][d].append(0.0)


                    elif obj != AIDSdeaths:
                        obj[i][j][k] = 0.0
                    else:
                        obj[i][j][k] = []
                        for l in xrange(minYear, maxYear + 1):
                            obj[i][j][k].append(0.0)


    AIDSdeathsCD4 = {}
    for age in xrange(0, 80, 5):
        AIDSdeathsCD4[age] = {}
        for t in xrange(minYear, maxYear+1):
            AIDSdeathsCD4[age][t] = {}
            for sex in sexes:
                AIDSdeathsCD4[age][t][sex] = {}
                if age >= 15:
                    for c in allCD4states:
                        if c in noARTCD4states:
                            AIDSdeathsCD4[age][t][sex][c] = 0
                        else:
                            AIDSdeathsCD4[age][t][sex][c] = {}
                            for d in adultARTdurations:
                                AIDSdeathsCD4[age][t][sex][c][d] = 0
                elif age < 15 and age >= 5:
                    for c in u15CD4States:
                        AIDSdeathsCD4[age][t][sex][c] = {}
                        for d in noARTChildHIVStates:
                            AIDSdeathsCD4[age][t][sex][c][d] = 0
                        AIDSdeathsCD4[age][t][sex][c]['ART'] = 0
                else:
                    for c in u5CD4States:
                        AIDSdeathsCD4[age][t][sex][c] = {}
                        for d in noARTChildHIVStates:
                            AIDSdeathsCD4[age][t][sex][c][d] = 0
                        AIDSdeathsCD4[age][t][sex][c]['ART'] = 0




    AIDSdeathsCD4[80] = {}
    for t in xrange(minYear, maxYear+1):
        AIDSdeathsCD4[80][t] = {}
        for sex in sexes:
            AIDSdeathsCD4[80][t][sex] = {}
            for c in allCD4states:
                if c in noARTCD4states:
                    AIDSdeathsCD4[80][t][sex][c] = 0
                else:
                    AIDSdeathsCD4[80][t][sex][c] = {}
                    for d in adultARTdurations:
                        AIDSdeathsCD4[80][t][sex][c][d] = 0


    eligibleSpecialPops = {}
    for c in noARTCD4states:
        eligibleSpecialPops[c] = {}
        for sex in sexes:
            eligibleSpecialPops[c][sex] = 0

    # Set hardcoded values
    retentionRateCountValues = [0.986, 0.799, 0.761, 0.757, 0.666, 0.644, 0.460, 0.50]
    if iteration_number == 0:
        adj_array = [1 + (normBounds(-10,10)/100) for i in xrange(len(retentionRateCountValues))]
        adjustments['retentionRateCountValues'] = adj_array
    else:
        adj_array = adjustments['retentionRateCountValues']
    retentionRateCountValues = [retentionRateCountValues[i] * adj_array[i] for i in xrange(len(retentionRateCountValues))]      
    retentionRateCounts = {}

    retentionRatePercentValues = [0.906, 0.759, 0.787, 0.795, 0.785, 0.756, 0.813, 0.700]
    if iteration_number == 0:
        adj_array = [1 + (normBounds(-10,10)/100) for i in xrange(len(retentionRatePercentValues))]
        adjustments['retentionRatePercentValues'] = adj_array
    else:
        adj_array = adjustments['retentionRatePercentValues']
    retentionRatePercentValues = [retentionRatePercentValues[i] * adj_array[i] for i in xrange(len(retentionRatePercentValues))]
    retentionRatePercents = {}
    for i in xrange(len(possibleCD4categories)):
        retentionRateCounts[possibleCD4categories[i]] = retentionRateCountValues[i]
        retentionRatePercents[possibleCD4categories[i]] = retentionRatePercentValues[i]

    noART15CD4distValues = [0.056, 0.112, 0.112, 0.07, 0.14, 0.23, 0.28]
    if iteration_number == 0:
        adj_array = [1 + (normBounds(-10,10)/100) for i in xrange(len(noART15CD4distValues))]
        adjustments['noART15CD4distValues'] = adj_array
    else:
        adj_array = adjustments['noART15CD4distValues']
    tmpNoART15CD4distValues = [noART15CD4distValues[i] * adj_array[i] for i in xrange(len(noART15CD4distValues))]
    noART15CD4distValues = [val / np.sum(tmpNoART15CD4distValues) for val in tmpNoART15CD4distValues]
    noART15CD4dist = {}
    for i in xrange(len(noART15CD4distValues)):
        noART15CD4dist[noARTCD4states[-(i+1)]] = noART15CD4distValues[i]

    ART15CD4distValues = [0.11, 0.23, 0.23, 0.14, 0.29]
    if iteration_number == 0:
        adj_array = [1 + (normBounds(-10,10)/100) for i in xrange(len(noART15CD4distValues))]
        adjustments['ART15CD4distValues'] = adj_array
    else:
        adj_array = adjustments['ART15CD4distValues']
    tmpART15CD4distValues = [ART15CD4distValues[i] * adj_array[i] for i in xrange(len(ART15CD4distValues))]
    ART15CD4distValues = [val / np.sum(tmpART15CD4distValues) for val in tmpART15CD4distValues]
    ART15CD4dist = {}
    for i in xrange(len(ART15CD4distValues)):
        ART15CD4dist[ARTCD4states[-(i+3)]] = ART15CD4distValues[i]

    # All positive HIV states
    allCD4states = list(noARTCD4states)
    allCD4states.extend(ARTCD4states)

    # All HIV states excluding 'all'
    # (Useful for iteration)
    allCD4statesNeg = list(allCD4states)
    allCD4statesNeg.append('neg')

    if iteration_number == 0:
        adj = 1 + (normBounds(-10,10)/100)
        adjustments['childPrSurvivalFY'] = adj
    else:
        adj = adjustments['childPrSurvivalFY']
    childPrSurvivalFY = min(1, 0.85 * adj)

    if iteration_number == 0:
        adj = 1 + (normBounds(-10,10)/100)
        adjustments['childPrSurvivalSY'] = adj
    else:
        adj = adjustments['childPrSurvivalSY']
    childPrSurvivalSY = min(1, 0.93 * adj)

    # In[33]:

    migration = {}
    for age in xrange(minAge, maxAge + 1):
        migration[age] = {}
        for t in xrange(minYear, maxYear + 1):
            migration[age][t] = {}

    if iteration_number == 0:
        adj_array = [1 + (normBounds(-10,10)/100) for row in inputMigrationData]
        adjustments['migration'] = adj_array
    else:
        adj_array = adjustments['migration']
    # Fill migration object, setting missing values to 0
    temp_i = 0
    if ISO not in ['IND', 'MDA']:
        for row in inputMigrationData:
            if int(row[yearIndexMig]) >= minYear and int(row[yearIndexMig]) <= maxYear:
                try:
                    migration[int(row[ageIndexMig])][int(row[yearIndexMig])][sexes[int(row[sexIndexMig])-1]] = float(row[valueIndexMig]) * adj_array[temp_i]
                    # migration[int(row[ageIndexMig])][int(row[yearIndexMig])][sexes[int(row[sexIndexMig])-1]] = float(row[valueIndexMig]) * 1.0
                except:
                    migration[int(row[ageIndexMig])][int(row[yearIndexMig])][sexes[int(row[sexIndexMig])-1]] = 0
                temp_i += 1
    else:
        for row in inputMigrationData:
            if int(row[yearIndexMig]) >= minYear and int(row[yearIndexMig]) <= maxYear:
                if int(row[ageIndexMig]) < 80:
                    try:
                        migration[int(row[ageIndexMig])][int(row[yearIndexMig])][sexes[int(row[sexIndexMig])-1]] = float(row[valueIndexMig]) * adj_array[temp_i]
                    except:
                        migration[int(row[ageIndexMig])][int(row[yearIndexMig])][sexes[int(row[sexIndexMig])-1]] = 0
                    temp_i += 1
                elif int(row[ageIndexMig]) == 80:
                    try:
                        migration[int(row[ageIndexMig])][int(row[yearIndexMig])][sexes[int(row[sexIndexMig])-1]] = float(row[valueIndexMig]) * adj_array[temp_i] * 5
                    except:
                        migration[int(row[ageIndexMig])][int(row[yearIndexMig])][sexes[int(row[sexIndexMig])-1]] = 0
                    temp_i += 1




    # Fill population object, interpolating if non-single age data are detected
    if int(inputPopData[2][ageIndexPop]) != int(inputPopData[0][ageIndexPop]) + 2:

        if iteration_number == 0:
            adj_object = [{k: 1 + (normBounds(-10,10)/100) for k in sexes} for a in xrange(0,81)]
            # adj_object = [{k: 1 + (normBounds(-10,10)/100) for k in sexes} for a in xrange(0,81)]
            adjustments['population'] = adj_object
        else:
            adj_object = adjustments['population']

        # Check if age 1 is included
        if int(inputPopData[1][ageIndexPop]) == 1:
            ageOne = True
        else:
            ageOne = False

        # Prepare containers to be used in interpolation
        # (Need a list for male and a list for females)
        popDataForInterpolation = {}
        interpolatedPopData = {}
        for sex in sexes:
            popDataForInterpolation[sex] = []

        # Reshape data into lists for interpolation
        for row in inputPopData:
            popDataForInterpolation[sexes[int(row[sexIndexPop])-1]].append(float(row[valueIndexPop]))

        # Combine age 0 with age 1, if necessary
        for sex in sexes:
            if ageOne:
                popDataForInterpolation[sex][0] += popDataForInterpolation[sex][1]
                popDataForInterpolation[sex].remove(popDataForInterpolation[sex][1])

        # Interpolate age groups 0 through 75, and append 80 at the end
        for sex in sexes:
            interpolatedPopData[sex] = beers.BeersInterpolateGroups(popDataForInterpolation[sex][:-1])
            for i in range(len(interpolatedPopData[sex])):
                interpolatedPopData[sex][i] = max(0, interpolatedPopData[sex][i])
            interpolatedPopData[sex].append(popDataForInterpolation[sex][-1])

        # Fill population object with interpolated data
        for age in xrange(0, 81):
            for sex in sexes:
                population[age][minYear][sex]['neg'] = interpolatedPopData[sex][age] * adj_object[age][sex]
                # population[age][minYear][sex]['neg'] = interpolatedPopData[sex][age] * 1
    # Fill object with single age input data, if interpolate isn't necessary 
    else:
        if iteration_number == 0:
            adj_array = [(1 + normBounds(-10,10)/100) for row in inputPopData]
            adjustments['population'] = adj_array
        else:
            adj_array = adjustments['population']

        temp_i = 0
        if ISO not in ['IND', 'MDA']:
            for row in inputPopData:
                try:
                    population[int(row[ageIndexPop])][int(row[yearIndexPop])][sexes[int(row[sexIndexPop])-1]]['neg'] = float(row[valueIndexPop]) * adj_array[temp_i]
                except:
                    population[int(row[ageIndexPop])][int(row[yearIndexPop])][sexes[int(row[sexIndexPop])-1]]['neg'] = 0
                temp_i += 1
        else:         
            for row in inputPopData:
                if int(row[ageIndexPop]) < 80:
                    try:
                        population[int(row[ageIndexPop])][int(row[yearIndexPop])][sexes[int(row[sexIndexPop])-1]]['neg'] = float(row[valueIndexPop]) * adj_array[temp_i]
                    except:
                        population[int(row[ageIndexPop])][int(row[yearIndexPop])][sexes[int(row[sexIndexPop])-1]]['neg'] = 0
                    temp_i += 1
                elif int(row[ageIndexPop]) == 80:
                    try:
                        population[int(row[ageIndexPop])][int(row[yearIndexPop])][sexes[int(row[sexIndexPop])-1]]['neg'] = float(row[valueIndexPop]) * adj_array[temp_i] * 5
                    except:
                        population[int(row[ageIndexPop])][int(row[yearIndexPop])][sexes[int(row[sexIndexPop])-1]]['neg'] = 0
                    temp_i += 1
                else:
                    pass

    if iteration_number == 0:
        adj_array = [(1 + normBounds(-10,10)/100) for row in inputTFRdata]
        adjustments['TFR'] = adj_array
    else:
        adj_array = adjustments['TFR']





    # Fill TFR object
    TFR = {}
    temp_i = 0
    for row in inputTFRdata:
        TFR[int(row[yearIndexTFR])] = float(row[valueIndexTFR]) * adj_array[temp_i]
        temp_i += 1


    # if locations['survivalRates'] == 'gbd_draws':
    draw_list_surv = [val for val in surv_vars if 'px' in val]
    draw_num_list_surv = [int(re.findall('\d+', val)[0]) for val in draw_list_surv]
    # max_draw_inc = max(draw_num_list_inc)
    n_surv_draws = len(draw_num_list_surv)

    draw_num_surv = draw_num_list_surv[(individual_id-1) % n_surv_draws]
    draw_col_surv = surv_vars.index('px' + str(draw_num_surv))
    survivalRates = {}
    for age in xrange(0, maxAge + 2):
        survivalRates[age] = {}
        for t in xrange(minYear, maxYear + 1):
            survivalRates[age][t] = {}



    for row in inputSRdata:
        if float(row[yearIndexSurv]) >= minYear and float(row[yearIndexSurv]) <= maxYear:
            # if locations['survivalRates'] != 'gbd_draws':
            # survivalRates[int(row[ageIndexSurv])][int(row[yearIndexSurv])][sexes[int(row[sexIndexSurv])-1]] = float(row[draw_col_surv])
            # else:
            survivalRates[int(row[ageIndexSurv])][float(row[yearIndexSurv])][row[sexIndexSurv]] = float(row[draw_col_surv])

    last_sr_year = max([float(row[yearIndexSurv]) for row in inputSRdata])
    if maxYear > last_sr_year:
        needed_years = range(last_sr_year+1, maxYear+1)
        last_year_data = [row for row in inputSRdata if float(row[yearIndexSurv]) == last_sr_year]
        for y in needed_years:
            for row in last_year_data:
                # if locations['survivalRates'] != 'gbd_draws':
                #   survivalRates[int(row[ageIndexSurv])][y][sexes[int(row[sexIndexSurv])-1]] = float(row[draw_col_surv])
                # else:
                survivalRates[int(row[ageIndexSurv])][y][row[sexIndexSurv]] = float(row[draw_col_surv])



    updateAllStateTotal(minYear)


    # Fill SRB object by converting SRB to percent female
    SRB = {}
    if iteration_number == 0:
        adj_array = [(1 + normBounds(-10,10)/100) for row in inputSRBdata]
        adjustments['SRB'] = adj_array
    else:
        adj_array = adjustments['SRB']
    temp_i = 0
    for row in inputSRBdata:
        SRB[int(row[yearIndexSRB])] = 1 / ((float(row[valueIndexSRB]) * adj_array[temp_i]) / 100 + 1)
        temp_i += 1




    # Calculate base-year births
    births = {}
    for age in xrange(15, 50):
        # Get five-year age group for ASFR
        a5 = age - age % 5
        sr = survivalRates[age-1][minYear]['female']
        births[age] = {}
        births[age][minYear] = {}
        births[age][minYear]['total'] = (population[age][minYear]['female']['all'] + population[age - 1][minYear]['female']['all'] * sr) / 2 * TFR[minYear] * (ASFRbyAge[a5][minYear - minYear] / np.sum(ASFRbyYear[minYear]) / 5)
        births[age][minYear]['female'] = births[age][minYear]['total'] * SRB[minYear]
        births[age][minYear]['male'] = births[age][minYear]['total'] * (1 - SRB[minYear])


    # Calculate base-year deaths
    for age in deaths:
        for sex in sexes:
            sr = survivalRates[age][minYear][sex]
            # Ages 1 through maxAge - 1
            if age > 0 and age < max(population):
                #capping migration at 80% pop
                if ((migration[age-1][minYear][sex] + migration[age][minYear][sex]) / 2) < 0:
                    mr = max(((migration[age-1][minYear][sex] + migration[age][minYear][sex]) / 2), -0.8*population[age-1][minYear][sex]['all']) 
                else:
                    mr = ((migration[age-1][minYear][sex] + migration[age][minYear][sex]) / 2)
                deaths[age][minYear][sex] = population[age-1][minYear][sex]['all'] * (1 - sr) + mr * (1 - sr) / 2
            # Age 80
            elif age == 80:
                if ((migration[79][minYear][sex] + migration[80][minYear][sex]) / 2) < 0:
                    mr = max(((migration[79][minYear][sex] + migration[80][minYear][sex]) / 2), -0.8*(population[79][minYear][sex]['all'] + population[80][minYear][sex]['all'] )) 
                else:
                    mr = (migration[79][minYear][sex] + migration[80][minYear][sex]) / 2
                deaths[age][minYear][sex] = (population[79][minYear][sex]['all'] + population[80][minYear][sex]['all']) * (1 - sr) + mr * (1 - sr)/2
            # Age 0
            else:
                deaths[age][minYear][sex] = totalByYear(births, minYear, sex) * (1 - sr) + 0.5 * (1 - sr) / 2

    updateAllStateTotal(minYear)

    ageIndex = inputNoARTMortality[0].index('age')
    drawIndex = inputNoARTMortality[0].index('draw')
    CD4Index = inputNoARTMortality[0].index('cd4')
    try:
        valueIndex = inputNoARTMortality[0].index('mort')
    except:
        valueIndex = inputNoARTMortality[0].index('prog')

    tmpNoARTmortality = [row for row in inputNoARTMortality[1:] if int(row[drawIndex]) == ((individual_id-1) % 1000 + 1)]

    # CD4 category lower counts
    progressionAges = [15,25,35,45]
    noARTmortality = {}
    for sex in sexes:
        noARTmortality[sex] = []
        for i in xrange(len(progressionAges)):
            noARTmortality[sex].append({})
            for c in noARTCD4states:
                noARTmortality[sex][i][c] = 0.0

    for sex in sexes:
        for row in tmpNoARTmortality:
            a1 = int(row[ageIndex][:2])
            age10 = ((a1 - (a1 - 5) % 10) - 15) / 10
            noARTmortality[sex][age10][row[CD4Index]] = float(row[valueIndex])


    durationIndex = inputOnARTMortality[0].index('durationart')
    CD4CatIndex = inputOnARTMortality[0].index('cd4_category')
    ageIndex = inputOnARTMortality[0].index('age')
    sexIndex = inputOnARTMortality[0].index('sex')

    draw_var_list = [val for val in inputOnARTMortality[0] if 'mort' in val]
    draw_num_list = [int(re.findall('\d+', val)[0]) for val in draw_var_list]
    max_draw_onartmort = max(draw_num_list)

    draw_num = (individual_id-1) % max_draw_onartmort + 1
    draw_col = inputOnARTMortality[0].index('mort' + str(draw_num))

    onARTmortality = {}
    for sex in sexes:
        onARTmortality[sex] = {}
        for d in adultARTdurations:
            onARTmortality[sex][d] = []
            for j in xrange(len(mortalityAgeCategories)):
                onARTmortality[sex][d].append({})

    for row in inputOnARTMortality[1:]:
        a1 = int(row[ageIndex][:2])
        age10 = ((a1 - (a1 - 5 ) % 10) - 15) / 10
        onARTmortality[sexes[int(row[sexIndex]) - 1]][row[durationIndex]][age10][row[CD4CatIndex]] = float(row[draw_col])

    ageIndex = inputProgressionParameters[0].index('age')
    drawIndex = inputProgressionParameters[0].index('draw')
    CD4Index = inputProgressionParameters[0].index('cd4')
    valueIndex = inputProgressionParameters[0].index('prog')

    tmpProgressionParameters = [row for row in inputProgressionParameters[1:] if int(row[drawIndex]) == ((individual_id-1) % 1000 + 1)]

    # CD4 category lower counts
    progressionAges = [15,25,35,45]
    progressionParameters = {}
    for age in progressionAges:
        progressionParameters[age] = {}
        for sex in sexes:
            progressionParameters[age][sex] = {}
            for c in noARTCD4states:
                progressionParameters[age][sex][c] = 0
                if c == 'LT50CD4':
                    progressionParameters[age][sex][c] = 0.0000001

    for sex in sexes:
        for row in tmpProgressionParameters:
            a1 = int(row[ageIndex][:2])
            progressionParameters[a1][sex][row[CD4Index]] = float(row[valueIndex])


    yearIndex = inputAdultARTeligibility[0].index('year')
    valueIndex = inputAdultARTeligibility[0].index('cd4_threshold')

    adultARTeligibility = [float(row[valueIndex]) for row in inputAdultARTeligibility[1:] if int(row[yearIndex]) <= maxYear]

    # CD4 category lower counts
    CD4lowerLimitValues = [0,50,100,200,250,350,500]
    CD4lowerLimits = {}
    for i in xrange(len(noARTCD4states)):
        CD4lowerLimits[noARTCD4states[i]] = CD4lowerLimitValues[i]

    ageIndex = inputTFRreduction[0].index('age')
    valueIndex = inputTFRreduction[0].index('tfr_ratio')

    if iteration_number == 0:
        adj_array = [(1 + normBounds(-10,10)/100) for row in inputTFRreduction[1:]]
        adjustments['TFRreduction'] = adj_array
    else:
        adj_array = adjustments['TFRreduction']
    TFRreduction = [float(inputTFRreduction[1:][i][valueIndex]) * adj_array[i] for i in xrange(len(inputTFRreduction[1:]))]
    for i in xrange(len(TFRreduction)):
        if TFRreduction[i] == 0:
            TFRreduction[i] = 1.0

    yearIndex = inputAdultARTCoverage[0].index('year')
    sexIndex = inputAdultARTCoverage[0].index('sex')
    numIndex = inputAdultARTCoverage[0].index('ART_cov_num')
    pctIndex = inputAdultARTCoverage[0].index('ART_cov_pct')
    pctGBDPopIndex = inputAdultARTCoverage[0].index('ART_cov_pct_total')

    firstYear = int(inputAdultARTCoverage[1][yearIndex])
    lastYear = int(inputAdultARTCoverage[-1][yearIndex])

    inputSexCoverage = {}
    inputSexCoverage["male"] = [row for row in inputAdultARTCoverage[1:] if int(row[sexIndex]) == 1 and int(row[yearIndex]) >= minYear and int(row[yearIndex]) <= maxYear]
    inputSexCoverage["female"] = [row for row in inputAdultARTCoverage[1:] if int(row[sexIndex]) == 2 and int(row[yearIndex]) >= minYear and int(row[yearIndex]) <= maxYear]

    adultARTCoverageType = ['num'] * len(inputSexCoverage["female"])
    for i in xrange(len(adultARTCoverageType)):
        if float(inputSexCoverage["female"][i][numIndex]) == 0 and float(inputSexCoverage["female"][i][pctIndex]) != 0:
            adultARTCoverageType[i] = 'percent'
    # for i in xrange(maxYear, minYear + len(adultARTCoverageType)):
    #   adultARTCoverageType[i-minYear] = adultARTCoverageType[2012-minYear]


    coverageFillerData = [0 for i in xrange(minYear, firstYear)]

    adultARTCoverage = {}
    for sex in sexes:
        adultARTCoverage[sex] = list(coverageFillerData)
        for i in range(len(adultARTCoverageType)):
            if adultARTCoverageType[i] == "percent":
                adultARTCoverage[sex].append(inputSexCoverage[sex][i][pctIndex])
            else:
                adultARTCoverage[sex].append(inputSexCoverage[sex][i][pctGBDPopIndex])
    # if firstYear < minYear:
    #     for sex in sexes:
    #         adultARTCoverage[sex] = adultARTCoverage[sex][minYear - firstYear:]
    for i in xrange(len(adultARTCoverageType)):
        if adultARTCoverageType[i] == 'percent':
            for sex in sexes:
                adultARTCoverage[sex][i] = min(100, adultARTCoverage[sex][i])

    # for sex in sexes:
    #   tempSum = 0
    #   for i in xrange(2012-4, maxYear):
    #     tempSum += (adultARTCoverage[sex][i-minYear] - adultARTCoverage[sex][i-minYear-1]) 
    #   tempMean = tempSum / 5
    #   for i in xrange(maxYear, lastYear + 1):
    #     adultARTCoverage[sex][i-minYear] = adultARTCoverage[sex][i-minYear-1] + tempMean

    if iteration_number == 0:
        adj_object = {k: (1 + normBounds(-10,10)/100) for k in adultARTCoverage}
        adjustments['adultARTCoverage'] = adj_object
    else:
        adj_object = adjustments['adultARTCoverage']

    for sex in adultARTCoverage:
        adj = adj_object[sex]
        for i in xrange(len(adultARTCoverage[sex])):
            adultARTCoverage[sex][i] = adultARTCoverage[sex][i] * adj



    outputART = []
    for t in xrange(minYear, maxYear + 1):
        tempART = 0
        for sex in sexes:
            tempART += adultARTCoverage[sex][t-minYear]
        outputART.append(tempART)
    t = minYear

    tempPMTCT = {}
    treatTypes = np.unique([var[:-4] for var in inputPMTCT[0] if var != 'year'])

    yearIndex = inputPMTCT[0].index('year')
    startYear = min([int(row[yearIndex]) for row in inputPMTCT[1:]])
    if minYear - startYear > 0:
        PMTCTshift = minYear - startYear
    else:
        PMTCTshift = 0

    tempPMTCT = {}
    percent_val_list = []
    treat_data_type = {}
    for p in treatTypes:
        treat_data_type[p] = []
        filler = [0 for i in xrange(minYear, startYear)]
        col_nums = [i for i in xrange(len(inputPMTCT[0])) if p in inputPMTCT[0][i]]
        pct_col = [i for i in xrange(len(inputPMTCT[0])) if inputPMTCT[0][i] == p+'_pct'][0]
        num_col = [i for i in xrange(len(inputPMTCT[0])) if inputPMTCT[0][i] == p+'_num'][0]
        tmp_num = np.array([float(row[num_col]) for row in inputPMTCT[1 + PMTCTshift:]])
        tmp_pct = np.array([float(row[pct_col]) for row in inputPMTCT[1 + PMTCTshift:]])


        percent_val_list.append(tmp_pct)
        for t in xrange(len(tmp_pct)):
            if tmp_num[t] != 0 and tmp_pct[t] == 0:
                treat_data_type[p].append('num')
            elif tmp_num[t] == 0 and tmp_pct[t] != 0:
                treat_data_type[p].append('percent')
            elif tmp_num[t] == 0 and tmp_pct[t] == 0:
                treat_data_type[p].append('num')
            elif tmp_num[t] != 0 and tmp_pct[t] != 0:
                treat_data_type[p].append('num')
                tmp_pct[t] = 0
        total = tmp_pct + tmp_num
        filler.extend(total)
        tmp_cov = list(filler)
        tempPMTCT[p] = tmp_cov
    PMTCTtype = {}
    PMTCTtype['prenat'] = {}
    PMTCTtype['postnat'] = {}
    for p in treat_data_type:
        filler = ['num' for i in xrange(minYear, startYear)]
        filler.extend(treat_data_type[p])
        if 'postnat' in p:
            PMTCTtype['postnat'][p.split('_')[-1]] = filler
        else:
            PMTCTtype['prenat'][p.split('_')[-1]] = filler

    prenatalProph = {}
    postnatalProph = {}
    for key in tempPMTCT:
        if 'postnat' in key:
            postnatalProph[key.split('_')[1]] = tempPMTCT[key]
        elif 'prenat' in key:
            prenatalProph[key.split('_')[1]] =  tempPMTCT[key]
        else:
            prenatalProph[key] = tempPMTCT[key]

    if iteration_number == 0:
        adj_object = {k: (1 + normBounds(-10,10)/100) for k in prenatalProph}
        adjustments['prenatalProph'] = adj_object
    else:
        adj_object = adjustments['prenatalProph']

    for treatType in prenatalProph:
        adj = adj_object[treatType]
        for y in xrange(len(prenatalProph[treatType])):
            prenatalProph[treatType][y] = prenatalProph[treatType][y] * adj

    if iteration_number == 0:
        adj_object = {k: (1 + normBounds(-10,10)/100) for k in postnatalProph}
        adjustments['postnatalProph'] = adj_object
    else:
        adj_object = adjustments['postnatalProph']
    for treatType in postnatalProph:
        adj = adj_object[treatType]
        for y in xrange(len(postnatalProph[treatType])):
            postnatalProph[treatType][y] = postnatalProph[treatType][y] * adj

    for p in prenatalProph:
        for y in xrange(len(prenatalProph[p])):
            if PMTCTtype['prenat'][p][y] == 'percent':
                prenatalProph[p][y] = min(100.0, prenatalProph[p][y])
    for p in postnatalProph:
        for y in xrange(len(postnatalProph[p])):
            if PMTCTtype['postnat'][p][y] == 'percent':
                postnatalProph[p][y] = min(100.0, postnatalProph[p][y])

    tmpPostnatalDropout = {}

    if iteration_number == 0:
        adj_object = {k: (1 + normBounds(-10,10)/100) for k in [j for j in inputPostnatalDropout[0] if j != 'year']}
        adjustments['postnatalDropout'] = adj_object
    else:
        adj_object = adjustments['postnatalDropout']
    for key in [k for k in inputPostnatalDropout[0] if k != 'year']:
        adj = adj_object[key]
        tmpPostnatalDropout[key.split('_')[-1]] = [float(row[inputPostnatalDropout[0].index(key)]) * adj for row in inputPostnatalDropout[1:]]

    dataStartYear = int(inputPostnatalDropout[1][0])
    postnatalDropout = {}
    for k in tmpPostnatalDropout:
        postnatalDropout[k] = [tmpPostnatalDropout[k][0]] * (dataStartYear - minYear)
        postnatalDropout[k].extend(tmpPostnatalDropout[k])

    # if locations['percentBF'] == 'defaults':
    #   valueIndex = inputPercentBF[0].index('notBF_pct')

    #   tempPercentBFvalues = [float(row[valueIndex]) for row in inputPercentBF[1:]]

    #   if iteration_number == 0:
    #     adj_array = [1 + normBounds(-10,10)/100 for i in xrange(len(tempPercentBFvalues))]
    #     adjustments['percentBFonART'] = adj_array
    #   else:
    #     adj_array = adjustments['percentBFonART']

    #   testPercentBF = {i+1: min(100, val * adj_array[i]) for i, val in zip(xrange(len(tempPercentBFvalues)), tempPercentBFvalues)}
    #   percentBFonART = [testPercentBF for i in xrange(minYear, maxYear+1)]
    #   percentBFnoART = [testPercentBF for i in xrange(minYear, maxYear+1)]

    # else:
    noARVindex = inputPercentBF[0].index('no_arv')
    onARVindex = inputPercentBF[0].index('on_arv')

    tmpNoARV = [float(row[noARVindex]) for row in inputPercentBF[1:]]
    tmpOnARV = [float(row[onARVindex]) for row in inputPercentBF[1:]]

    if iteration_number == 0:
        adj_array = [1 + normBounds(-10,10)/100 for i in xrange(len(tmpNoARV))]
        adjustments['percentBFnoART'] = adj_array
    else:
        adj_array = adjustments['percentBFnoART']
    percentBFnoARTvalues = {i+1: min(100, val * adj_array[i]) for i, val in zip(xrange(len(tmpNoARV)), tmpNoARV)}

    if iteration_number == 0:
        adj_array = [1 + normBounds(-10,10)/100 for i in xrange(len(tmpOnARV))]
        adjustments['percentBFonART'] = adj_array
    else:
        adj_array = adjustments['percentBFonART']       
    percentBFonARTvalues = {i+1: min(100, val * adj_array[i]) for i, val in zip(xrange(len(tmpOnARV)), tmpOnARV)}
    percentBFonART = [percentBFonARTvalues for i in xrange(minYear, maxYear+1)]
    percentBFnoART = [percentBFnoARTvalues for i in xrange(minYear, maxYear+1)]


    # Create HIV-specific data containers
    eligibleAdults = []
    for i in xrange(minYear, maxYear+1):
        eligibleAdults.append({})
        for sex in sexes:
            eligibleAdults[i-minYear][sex] = 0
    eligibleAdultsCD4 = {}
    for i in xrange(len(noARTCD4states)):
        eligibleAdultsCD4[noARTCD4states[i]] = {}
        for sex in sexes:
            eligibleAdultsCD4[noARTCD4states[i]][sex] = 0

    currentYearART = {}
    prevYearART = {}
    twoPrevYearsART = {}
    for obj in [currentYearART, prevYearART, twoPrevYearsART]:
        for sex in sexes:
            obj[sex] = 0 

    # Number of increments in a year
    timeStep = 10

    eligByAge = []
    for i in xrange(4):
        eligByAge.append({})
        for c in xrange(len(noARTCD4states)):
            eligByAge[i][noARTCD4states[c]] = {}
            for sex in sexes:
                eligByAge[i][noARTCD4states[c]][sex] = 0

    mortRate = {}
    for c in noARTCD4states:
        mortRate[c] = 0

    newPatients = {}
    for c in noARTCD4states:
        newPatients[c] = {}
        for sex in sexes:
            newPatients[c][sex] = 0

    entrants = {}
    exits = {}
    for obj in [entrants, exits]:
        for c in allCD4states:
            obj[c] = {}
            if c in noARTCD4states:
                obj[c][1] = []
                for i in xrange(minYear, maxYear+1):
                    obj[c][1].append(0.0)
            else:
                for d in adultARTdurations:
                    obj[c][d] = []
                    for i in xrange(minYear, maxYear+1):
                        obj[c][d].append(0.0)

    # Use generalized age pattern of incidence
    # Create age distribution object
    # (As of right now, it is the same for each year)
    ageIndex = inputIncAgeDist[0].index('age')
    sexIndex = inputIncAgeDist[0].index('sex')
    upperIndex = inputIncAgeDist[0].index('upper')
    lowerIndex = inputIncAgeDist[0].index('lower')

    tmp_incAgeDist_SY = {}
    incAgeDist_SY = {}
    for sex in sexes:
        tmp_incAgeDist_SY[sex] = {}
        incAgeDist_SY[sex] = {}
        for age in xrange(0, maxAge + 1, 5):
            tmp_incAgeDist_SY[sex][age] = 0
            incAgeDist_SY[sex][age] = 0

    for row in inputIncAgeDist[1:]:
        a1 = int(row[ageIndex][:2])
        tmp_incAgeDist_SY[sexes[int(row[sexIndex]) - 1]][a1] = random.uniform(float(row[lowerIndex]), float(row[upperIndex]))

    for sex in sexes:
        tmp25 = tmp_incAgeDist_SY[sex][25]
        for age in tmp_incAgeDist_SY[sex]:
            incAgeDist_SY[sex][age] = tmp_incAgeDist_SY[sex][age] / tmp25

    incAgeDist = {}
    for sex in sexes:
        incAgeDist[sex] = []
        for t in xrange(minYear, maxYear + 1):
            incAgeDist[sex].append({})
            for age in xrange(0, maxAge + 1, 5):
                incAgeDist[sex][t-minYear][age] = incAgeDist_SY[sex][age]

    variables_inc = inputEPPinc[0]
    draw_list_inc = [val for val in variables_inc if 'draw' in val]
    draw_num_list_inc = [int(re.findall('\d+', val)[0]) for val in draw_list_inc]
    max_draw_inc = max(draw_num_list_inc)

    draw_num_inc = (individual_id-1) % max_draw_inc + 1
    draw_col_inc = variables_inc.index('draw' + str(draw_num_inc))

    variables_prev = inputEPPprev[0]
    draw_list_prev = [val for val in variables_prev if 'draw' in val]
    draw_num_list_prev = [int(re.findall('\d+', val)[0]) for val in draw_list_prev]
    max_draw_prev = max(draw_num_list_prev)

    draw_num_prev = (individual_id-1) % max_draw_prev + 1
    draw_col_prev = variables_prev.index('draw' + str(draw_num_prev))

    prev_data = [float(row[draw_col_prev]) for row in inputEPPprev[1:]]
    #If stage 1, we read in year - specific incidence, which we split later
    inc_data = [float(row[draw_col_inc]) for row in inputEPPinc[1:]]
    yearIndex = variables_inc.index('year')
    years = [int(row[yearIndex]) for row in inputEPPinc[1:]]

    EPPdata = {row[0]: {'inc': row[1], 'prev': row[2]} for row in zip(years, inc_data, prev_data)}
    #If stage 2, we read in year/sex/age - specific incidence
    #if stage == 'stage_2' or group[0] == '2': 
    if stage == 'stage_2' or group[0] == '2': 
        variables_adj_inc = inputAdjInc[0]
        draw_list_adj_inc = [val for val in variables_adj_inc if 'draw' in val]
        draw_num_list_adj_inc = [int(re.findall('\d+', val)[0]) for val in draw_list_adj_inc]
        max_draw_adj_inc = max(draw_num_list_adj_inc)

        draw_num_adj_inc = (individual_id-1) % max_draw_adj_inc + 1
        draw_col_adj_inc = variables_adj_inc.index('draw' + str(draw_num_adj_inc))
        sexIndex = variables_adj_inc.index('sex')
        ageIndex = variables_adj_inc.index('single.age')
        yearIndex = variables_adj_inc.index('year')
        incByAgeSex = {}
        for year in range(minYear, maxYear + 1):
            incByAgeSex[year] = {}
            for sex in sexes:
                incByAgeSex[year][sex] = {x:0 for x in range(15, maxAge + 1)}
        for row in inputAdjInc[1:]:
            age = int(row[ageIndex])
            year = int(row[yearIndex])
            sex = row[sexIndex]
            if year >= minYear:
                if stage == 'stage_2':
                    incByAgeSex[year][sex][age] = float(row[draw_col_adj_inc]) /100
                else:
                    incByAgeSex[year][sex][age] = float(row[draw_col_adj_inc])


    yearIndex = inputIncSexRatio[0].index('year')
    valueIndex = inputIncSexRatio[0].index('FtoM_inc_ratio')
    if group[0] != '1':
        incSexRatioData = [row for row in inputIncSexRatio[1:] if row[1] == epidemicType]
        if iteration_number == 0:
            adj = 1 + normBounds(-20,20)/100
            adjustments['incSexRatio'] = adj
        else:
            adj = adjustments['incSexRatio']
        incSexRatio = [.24 * sexRatioAdj for i in xrange(minYear, maxYear + 1)]
    else:
        incSexRatioData = [row for row in inputIncSexRatio[1:] if row[0] == str(run_num)]
        adj = 1
        startYearRatio = float(float([row for row in incSexRatioData if row[yearIndex] == '1'][0][valueIndex]))
        incSexRatio = [startYearRatio for i in xrange(minYear, maxYear + 1)]


    # Set initial values
    



    # Replace inital values with actual values
    for y in xrange(epidemicStartYear, maxYear + 1):
        incSexRatio[y-minYear] = float(incSexRatioData[min(y-epidemicStartYear, len(incSexRatioData)-1)][valueIndex]) * adj * sexRatioAdj

    CD4CatIndex = inputInitCD4dist[0].index('CD4_category')
    ageIndex = inputInitCD4dist[0].index('age')
    valueIndex = inputInitCD4dist[0].index('Pct_new_infections')
    sexIndex = inputInitCD4dist[0].index('sex')

    sexes = ['male', 'female']
    noARTCD4states = ['LT50CD4', '50to99CD4', '100to199CD4', '200to249CD4',
        '250to349CD4', '350to500CD4', 'GT500CD4']

    initCD4dist = []
    for i in xrange(4):
        initCD4dist.append({})
        for c in noARTCD4states:
            initCD4dist[i][c] = {}


    for row in inputInitCD4dist[1:]:
        a1 = int(row[ageIndex][:2])
        age10 = ((a1 - (a1 - 5) % 10) - 15) / 10
        initCD4dist[age10][row[CD4CatIndex]][sexes[int(row[sexIndex])-1]] = float(row[valueIndex])

    if iteration_number == 0:
        adj_array = [1 + normBounds(-10,10)/100 for a in xrange(len(initCD4dist))]
        adjustments['initCD4dist'] = adj_array
    else:
        adj_array = adjustments['initCD4dist']
    for s in sexes:
        for a in xrange(len(initCD4dist)):
            initCD4dist[a]['GT500CD4'][s] = initCD4dist[a]['GT500CD4'][s] * adj_array[a]
            initCD4dist[a]['350to500CD4'][s] = 100 - initCD4dist[a]['GT500CD4'][s]

    MTCtransRates = {}
    transTypes = ['LT200', '200to350', 'GT350', 'IncidentInf', 'singleDoseNevir', 'dualARV', 'optionA', 'optionB', 'tripleARTbefPreg', 'tripleARTdurPreg']

    if iteration_number == 0:
        adj_object = [{k: 1 + normBounds(-10,10)/100 for k in ['perinatal', 'BFLT350', 'BFGE350']} for i in xrange(len(inputMTCtransRates[1:]))]
        adjustments['MTCtransRates'] = adj_object
    else:
        adj_object = adjustments['MTCtransRates']

    for i in xrange(len(inputMTCtransRates[1:])):
        MTCtransRates[transTypes[i]] = {}
        for inf in ['perinatal', 'BFLT350', 'BFGT350']:
            typeIndex = [inputMTCtransRates[0].index(val) for val in inputMTCtransRates[0] if inf.lower()[2:] in val][0]
            infname = inf
            if inf == 'BFGT350':
                infname = 'BFGE350'
            try:
                MTCtransRates[transTypes[i]][infname] = float(inputMTCtransRates[1:][i][typeIndex]) / 100 * adj_object[i][infname]
            except:
                MTCtransRates[transTypes[i]][infname] = inputMTCtransRates[1:][i][typeIndex]

    childCD4countDist = {}
    for i in xrange(minYear + 1, maxYear + 1):
        childCD4countDist[i] = []
    for j in xrange(len(childCD4countDist)):
        for i in xrange(len(inputChildCD4countDist)):
            childCD4countDist[j + minYear + 1].append({})
            for c in xrange(len(inputChildCD4countDist[i][1:])):
                if j == 0:
                    childCD4countDist[j + minYear + 1][i][possibleCD4categories[c]] = float(inputChildCD4countDist[i][1:][c])
                else:
                    childCD4countDist[j + minYear + 1][i][possibleCD4categories[c]] = 0.0

    if iteration_number == 0:
        adj_object = [{k: 1 + normBounds(-10,10)/100 for k in childCD4countDist[minYear+1][a]} for a in xrange(len(childCD4countDist[minYear+1]))]
        adjustments['childCD4countDist'] = adj_object
    else:
        adj_object = adjustments['childCD4countDist']

    for a in xrange(len(childCD4countDist[minYear+1])):
        tmpDist = {}
        for c in childCD4countDist[minYear+1][a]:
            tmpDist[c] = childCD4countDist[minYear+1][a][c] * adj_object[a][c]
        for c in childCD4countDist[minYear+1][a]:
            childCD4countDist[minYear+1][a][c] = tmpDist[c] / sum(tmpDist.values()) * 100

    childCD4percentDist = {}
    for i in xrange(minYear + 1, maxYear + 1):
        childCD4percentDist[i] = []
    for j in xrange(len(childCD4percentDist)):
        for i in xrange(len(inputChildCD4percentDist)):
            childCD4percentDist[j + minYear + 1].append({})
            for c in xrange(len(inputChildCD4percentDist[i][1:])):
                if j == 0:
                    childCD4percentDist[j + minYear + 1][i][possibleCD4categories[c]] = float(inputChildCD4percentDist[i][1:][c])
                else:
                    childCD4percentDist[j + minYear + 1][i][possibleCD4categories[c]] = 0.0

    if iteration_number == 0:
        adj_object = [{k: 1 + normBounds(-10,10)/100 for k in childCD4percentDist[minYear+1][a]} for a in xrange(len(childCD4percentDist[minYear+1]))]
        adjustments['childCD4percentDist'] = adj_object
    else:
        adj_object = adjustments['childCD4percentDist']

    for a in xrange(len(childCD4percentDist[minYear+1])):
        tmpDist = {}
        for c in childCD4percentDist[minYear+1][a]:
            tmpDist[c] = childCD4percentDist[minYear+1][a][c] * adj_object[a][c]
        for c in childCD4percentDist[minYear+1][a]:
            childCD4percentDist[minYear+1][a][c] = tmpDist[c] / sum(tmpDist.values()) * 100

    ageThresholdIndex = inputChildARTelgibility[0].index('age_below_all_treat_mos')
    countThresholdIndex = inputChildARTelgibility[0].index('cd4_count_thresh')
    percentThresholdIndex = inputChildARTelgibility[0].index('cd4_pct_thresh')
    ageIndex = inputChildARTelgibility[0].index('age')
    yearIndex = inputChildARTelgibility[0].index('year')

    childEligibilityAge = [float(row[ageThresholdIndex]) for row in inputChildARTelgibility[1:] if row[ageIndex] == '12to35mos']

    orderedAges = ['LT11mos', '12to35mos', '35to59mos', 'GT5yrs']

    childEligibilityCount = []
    childEligibilityPercent = []
    for i in xrange(len(orderedAges)):
        childEligibilityCount.append([float(row[countThresholdIndex]) for row in inputChildARTelgibility[1:] if row[ageIndex] == orderedAges[i]])
        childEligibilityPercent.append([float(row[percentThresholdIndex]) for row in inputChildARTelgibility[1:] if row[ageIndex] == orderedAges[i]])

    yearIndex = inputChildARTCoverage[0].index('year')
    ARTnumIndex = inputChildARTCoverage[0].index('ART_cov_num')
    CTXnumIndex = inputChildARTCoverage[0].index('Cotrim_cov_num')
    ARTpctIndex = inputChildARTCoverage[0].index('ART_cov_pct')
    CTXpctIndex = inputChildARTCoverage[0].index('Cotrim_cov_pct')

    firstYear = int(inputChildARTCoverage[1][yearIndex])

    coverageFillerData = [0 for i in xrange(minYear, firstYear)]

    if iteration_number == 0:
        adj_object = {p: [1 + normBounds(-10,10)/100 for i in inputChildARTCoverage[1:]] for p in ['num', 'pct']}
        adjustments['childARTcoverageCTX'] = adj_object
    else:
        adj_object = adjustments['childARTcoverageCTX']

    childARTcoverage = {}
    childARTcoverage['CTX'] = list(coverageFillerData)
    childARTcoverage['CTX'].extend([float(inputChildARTCoverage[1:][i][CTXnumIndex]) * adj_object['num'][i] + min(100,float(inputChildARTCoverage[1:][i][CTXpctIndex]) * adj_object['pct'][i]) for i in xrange(len(inputChildARTCoverage[1:]))])

    if iteration_number == 0:
        adj_object = {p: [1 + normBounds(-10,10)/100 for i in inputChildARTCoverage[1:]] for p in ['num', 'pct']}
        adjustments['childARTcoverageART'] = adj_object
    else:
        adj_object = adjustments['childARTcoverageART']     
    childARTcoverage['ART'] = list(coverageFillerData)
    childARTcoverage['ART'].extend([float(inputChildARTCoverage[1:][i][ARTnumIndex]) * adj_object['num'][i] + min(100,float(inputChildARTCoverage[1:][i][ARTpctIndex]) * adj_object['pct'][i]) for i in xrange(len(inputChildARTCoverage[1:]))])

    childARTcoverageType = {}
    for i in ['CTX', 'ART']:
        childARTcoverageType[i] = ['num' for x in xrange(minYear, firstYear)]

    childARTcoverageType['CTX'].extend(['percent' if float(row[CTXpctIndex]) > 0 else 'num' for row in inputChildARTCoverage[1:]])
    childARTcoverageType['ART'].extend(['percent' if float(row[ARTpctIndex]) > 0 else 'num' for row in inputChildARTCoverage[1:]])

    childInfectionCategories = ['perinatal', 'postnatal0to180', 'postnatal181to365', 'postnatal365+']
    valueIndices = {}


    yearIndex = inputChildCTXeffect[0].index('year_fm_start')
    statusIndex = inputChildCTXeffect[0].index('ART_status')
    valueIndex = inputChildCTXeffect[0].index('hivmort_reduction')

    childCTXeffect = {}
    childCTXeffect['noART'] = []
    childCTXeffect['onART'] = []

    for row in inputChildCTXeffect[1:]:
        if row[statusIndex] == 'noART':
            childCTXeffect['noART'].append(float(row[valueIndex]))
        else:
            childCTXeffect['onART'].append(float(row[valueIndex]))

    if iteration_number == 0:
        adj_object = {k: [1 + (normBounds(-10,10)/100) for i in childCTXeffect[k]] for k in childCTXeffect}
        adjustments['childCTXeffect'] = adj_object
    else:
        adj_object = adjustments['childCTXeffect']

    for k in childCTXeffect:
        for a in xrange(len(childCTXeffect[k])):
            childCTXeffect[k][a] = childCTXeffect[k][a] * adj_object[k][a]
    #Child update Sept 2017 from DPPROJ.PAS
    #We dont know how long anyone has been on cotrim
    childCTXeffect['noART'] = sum([childCTXeffect['noART'][d] for d in range(0,5)])/5
    childCTXeffect['onART'] = sum([childCTXeffect['onART'][d] for d in range(0,5)])/5 

    eligiblePopsData = [row for row in inputEligiblePops if row[col(inputEligiblePops, 'iso3')] == ISO]

    eligiblePopTuples = [(row[col(inputEligiblePops, 'pop_eligible')], row[col(inputEligiblePops, 'year_implemented')], row[col(inputEligiblePops, 'estimated_pct_HIVpos')]) for row in eligiblePopsData if row[col(inputEligiblePops, 'eligible')] == 'Y']
    eligiblePops = [tup[0] for tup in eligiblePopTuples]
    eligiblePopYears = {k: int(v) for (k,v,y) in eligiblePopTuples}
    eligiblePopPercents = {k: float(v) / 100 for (k,y,v) in eligiblePopTuples if k != "pregnant_women"}

    need15plus = {}
    for year in xrange(minYear, maxYear+1):
        need15plus[year] = {}
        for sex in sexes:
            need15plus[year][sex] = 0
    t = minYear

    # Write initial data to csvData list
    out_cats = ['neg', 'LT200CD4', '200to350CD4', 'GT350CD4', 'ART']


    t=minYear
    for a in xrange(0, maxAge+1):
        for s in sex_nums.values():
            tmp_row = [individual_id, t, a, s]
            out_deaths = [0.0 for i in xrange(minYear, maxYear+1)]
            tmp_row.extend(out_deaths)
            cohortData.append(tmp_row)

    tmpPop = {}
    for sex in sexes:
        for age in xrange(0, 15, 5):
            tmpBirthPrev = 0
            popData = []
            tmpNewHIV = 0
            tmpDeaths = 0
            tmpHIVbirths = 0
            tmpTotalBirths = 0
            tmpNonAIDSdeaths = 0
            HivDARTData = []
            tmpHivDART = {}
            HivDnoARTData = []
            tmpHivDnoART = {}
            PonARTData = []
            tmpPonART = {}
            PnoARTData = []
            tmpPnoART = {}
            for a in ARTCD4states:
                tmpHivDART[a] = 0
                HivDARTData.append(tmpHivDART[a])
            for b in noARTCD4states:
                tmpHivDnoART[b] = 0
                HivDnoARTData.append(tmpHivDnoART[b])
            for a5 in xrange(age, age + 5):
                tmpNonAIDSdeaths += deaths[a5][t][sex]
            for c in ['neg', 'LT200CD4', '200to350CD4', 'GT350CD4', 'ART']:
                tmpPop[c] = 0
                for a5 in xrange(age, age + 5):
                    if c == 'neg':
                        tmpPop[c] += population[a5][t][sex][c]
                popData.append(tmpPop[c])
            for p in ARTCD4states:
                tmpPonART[p] = 0
                PonARTData.append(tmpPonART[p])
            for q in noARTCD4states:
                tmpPnoART[q] = 0
                PnoARTData.append(tmpPnoART[q])
            out_data = [individual_id, t, sex, age, tmpDeaths, tmpNewHIV, tmpHIVbirths, np.sum(popData), tmpNonAIDSdeaths, tmpTotalBirths, tmpBirthPrev]
            out_data.extend(popData)
            csvData.append(out_data)
        for age in xrange(15, 80, 5):
            tmpBirthPrev = 0
            popData = []
            tmpNewHIV = 0
            tmpDeaths = 0
            tmpHIVbirths = 0
            tmpTotalBirths = 0
            if sex == 'female' and age < 50:
                tmpTotalBirths = births[age][t]['total']
            tmpNonAIDSdeaths = 0
            HivDARTData = []
            tmpHivDART = {}
            HivDnoARTData = []
            tmpHivDnoART = {}
            PonARTData = []
            tmpPonART = {}
            PnoARTData = []
            tmpPnoART = {}
            for a in ARTCD4states:
                tmpHivDART[a] = 0
                HivDARTData.append(tmpHivDART[a])
            for b in noARTCD4states:
                tmpHivDnoART[b] = 0
                HivDnoARTData.append(tmpHivDnoART[b])
            for a5 in xrange(age, age + 5):
                tmpNonAIDSdeaths += deaths[a5][t][sex]
            for c in ['neg', 'LT200CD4', '200to350CD4', 'GT350CD4', 'ART']:
                tmpPop[c] = 0
                for a5 in xrange(age, age + 5):
                    if c == 'neg':
                        tmpPop[c] += population[a5][t][sex][c]
                popData.append(tmpPop[c])
            for p in ARTCD4states:
                tmpPonART[p] = 0
                PonARTData.append(tmpPonART[p])
            for q in noARTCD4states:
                tmpPnoART[q] = 0
                PnoARTData.append(tmpPnoART[q])
            out_data = [individual_id, t, sex, age, tmpDeaths, tmpNewHIV, tmpHIVbirths, np.sum(popData), tmpNonAIDSdeaths, tmpTotalBirths, tmpBirthPrev]
            out_data.extend(popData)
            # out_data.extend(HivDARTData)
            # out_data.extend(HivDnoARTData)
            # out_data.extend(PonARTData)
            # out_data.extend(PnoARTData)
            csvData.append(out_data)
        age = 80
        popData = []
        tmpNewHIV = 0
        tmpDeaths = 0
        tmpHIVbirths = 0
        tmpTotalBirths = 0
        tmpNonAIDSdeaths = 0
        tmpNonAIDSdeaths = deaths[age][t][sex]
        HivDARTData = []
        tmpHivDART = {}
        HivDnoARTData = []
        tmpHivDnoART = {}
        PonARTData = []
        tmpPonART = {}
        PnoARTData = []
        tmpPnoART = {}
        for a in ARTCD4states:
            tmpHivDART[a] = 0
            HivDARTData.append(tmpHivDART[a])
        for b in noARTCD4states:
            tmpHivDnoART[b] = 0
            HivDnoARTData.append(tmpHivDnoART[b])
        for p in ARTCD4states:
            tmpPonART[p] = 0
            PonARTData.append(tmpPonART[p])
        for q in noARTCD4states:
            tmpPnoART[q] = 0
            PnoARTData.append(tmpPnoART[q]) 
        for c in ['neg', 'LT200CD4', '200to350CD4', 'GT350CD4', 'ART']:
            tmpPop[c] = 0
            if c == 'neg':
                tmpPop[c] += population[age][t][sex][c]
            popData.append(tmpPop[c])
        out_data = [individual_id, t, sex, age, tmpDeaths, tmpNewHIV, tmpHIVbirths, np.sum(popData), tmpNonAIDSdeaths, tmpTotalBirths, tmpBirthPrev]
        out_data.extend(popData)
        csvData.append(out_data)


    perinatalTransmission = []
    tempTotalPopByAge = {}

    allInterventionCoverage = {}
    for t in xrange(minYear, maxYear + 1):
        allInterventionCoverage[t] = {}
        for a in ['adult', 'child']:
            allInterventionCoverage[t][a] = {}
            for sex in sexes:
                allInterventionCoverage[t][a][sex] = {}
                for c in allInterventions:
                    allInterventionCoverage[t][a][sex][c] = {}
                    for e in ['coverage', 'eligible']:
                        allInterventionCoverage[t][a][sex][c][e] = 0

              
    if stage == 'stage_2' and parent != 'IND':
        childIncAdjPD = pd.read_csv("/ihme/hiv/ciba_temp/" + folder + "_ratios/"+ISO+"_inc_ratios.csv")
        childIncAdjPD = childIncAdjPD[['sex','single.age','year','ratio_'+str(individual_id)]]
        childIncAdjPD = childIncAdjPD.rename(columns = {'ratio_'+str(individual_id) : 'value'})
        childIncAdj = {}
        for year in range(minYear, 2020):
            childIncAdj[year] = {}
            for age in range(0,15):
                childIncAdj[year][age] = {}
                for sex in sexes:
                    childIncAdj[year][age][sex] = childIncAdjPD.loc[(childIncAdjPD['year'] == year) & (childIncAdjPD['sex'] == sex) & (childIncAdjPD['single.age'] == age) ].value.item()

      


    t = minYear
    for a in ['adult', 'child']:
        for sex in sexes:
            for c in allInterventions:
                coverageData.append([individual_id, t, a, sex, c, allInterventionCoverage[t][a][sex][c]['coverage'], allInterventionCoverage[t][a][sex][c]['eligible']])
    newChildART = {}
    for age in range(0, 15):
        newChildART[age] = {}
        for sex in sexes:
            newChildART[age][sex] = {}
            if age < 5:
                CD4States = u5CD4States
            else:
                CD4States = u15CD4States
            for c in CD4States:
                newChildART[age][sex][c] = {}
                for d in noARTChildHIVStates:
                    newChildART[age][sex][c][d] = 0

    # Predicted Coverage
    if usePredCoverage == True:  
        predARTCoverage = readPredARTCoverage(ISO)

    # ## Projection

    # ## Projection

    # In[6]:

    for t in xrange(minYear + 1, maxYear + 1):
    #     t = minYear + 1
        print t
        newChildHIV = {}
        for sex in sexes:
            newChildHIV[sex] = {}
            if output_type == 'five_year':
                for age in xrange(0, 15, 5):
                    newChildHIV[sex][age] = 0
            elif output_type == 'single_year':
                for age in xrange(0, 15):
                    newChildHIV[sex][age] = 0
        birthsToPosMothers = {}
        for sex in sexes:
            birthsToPosMothers[sex] = {}
            if output_type == 'five_year':
                for age in xrange(0, 80, 5):
                    birthsToPosMothers[sex][age] = 0
                birthsToPosMothers[sex][80] = 0
            elif output_type == 'single_year':
                for age in xrange(0, 81):
                    birthsToPosMothers[sex][age] = 0
        birthsToAllMothers = {}
        for sex in sexes:
            birthsToAllMothers[sex] = {}
            if output_type == 'five_year':
                for age in xrange(0, 80, 5):
                    birthsToAllMothers[sex][age] = 0
                birthsToAllMothers[sex][80] = 0
            elif output_type == 'single_year':
                for age in xrange(0, 81):
                    birthsToAllMothers[sex][age] = 0

        tmpPop70noART = 0
        for sex in sexes:
            for age in population: 
                # Age uninfected
                if age > 0 and age < max(population):
                    ## Project deaths for ages 1 through maxAge - 1
                    # le = roundToTenth(LE[t][sex])
                    # sr = survivalRatesByAge[age][le][sex]
                    sr = survivalRates[age][t][sex]

                    if ((migration[age-1][t][sex] + migration[age][t][sex]) / 2) < 0:
                        mr = max(((migration[age-1][t][sex] + migration[age][t][sex]) / 2), -0.8*population[age-1][t-1][sex]['all'])
                    else:
                        mr = ((migration[age-1][t][sex] + migration[age][t][sex]) / 2)                
                    deaths[age][t][sex] = population[age-1][t-1][sex]['all'] * (1 - sr) + mr * (1 - sr) / 2

                    ## Project population for ages 1 through maxAge - 1
                    if population[age-1][t-1][sex]['all'] < 1:
                        mr = 0
                    else:                    
                        mr = mr / population[age-1][t-1][sex]['all']
                    population[age][t][sex]['neg'] = population[age-1][t-1][sex]['neg'] * sr + population[age-1][t-1][sex]['neg'] * mr * (1 + sr)/2
                    if population[age][t][sex]['neg'] < 0:
                        population[age][t][sex]['neg'] = 0 

                # Age infected by age group
                # Children under 5
                if age <= 4 and age >0:
                    for c in u5CD4States:
                        for d in noARTChildHIVStates:
                            for i in range(minYear, t):
                                population[age][t][sex][c][d][i-minYear] = population[age-1][t-1][sex][c][d][i-minYear] * sr + population[age-1][t-1][sex][c][d][i-minYear] * mr * (1 + sr)/2
                                if population[age][t][sex][c][d][i-minYear] < 0:
                                    population[age][t][sex][c][d][i-minYear] = 0
                        for d in adultARTdurations:
                            for i in xrange(minYear, t):
                                population[age][t][sex][c][d][i-minYear] = population[age-1][t-1][sex][c][d][i-minYear] * sr + population[age-1][t-1][sex][c][d][i-minYear] * mr * (1 + sr)/2
                                if population[age][t][sex][c][d][i-minYear] < 0:
                                    population[age][t][sex][c][d][i-minYear] = 0
                #5 year olds
                if age == 5:
                    #Convert from CD4 percent to CD4 count
                    #Fractions from DPPROJ.PAS
                    for d in noARTChildHIVStates + adultARTdurations:
                        for i in xrange(minYear, t):
                            population[age][t][sex]["LT200CD4"][d][i-minYear] = population[age - 1][t - 1][sex]["LT5"][d][i-minYear] + population[age - 1][t - 1][sex]["5to10"][d][i-minYear] + (0.111 * population[age - 1][t - 1][sex]["11to15"][d][i-minYear] )
                            population[age][t][sex]["200to349CD4"][d][i-minYear] = (0.889 * population[age - 1][t - 1][sex]["11to15"][d][i-minYear]) + (.0231 * population[age - 1][t - 1][sex]["16to20"][d][i-minYear])
                            population[age][t][sex]["350to499CD4"][d][i-minYear] = (0.7693 * population[age - 1][t - 1][sex]["16to20"][d][i-minYear]) + (0.1667 * population[age - 1][t - 1][sex]["21to25"][d][i-minYear])  
                            population[age][t][sex]["500to749CD4"][d][i-minYear] = (0.8333 * population[age - 1][t - 1][sex]["21to25"][d][i-minYear]) + (0.4 * population[age -1][t - 1][sex]["26to30"][d][i-minYear])
                            population[age][t][sex]["750to999CD4"][d][i-minYear] = (0.6 * population[age - 1][t - 1][sex]["26to30"][d][i-minYear]) + (0.2857 * population[age - 1][t - 1][sex]["GT30"][d][i-minYear])
                            population[age][t][sex]["GT1000CD4"][d][i-minYear] = .7143 * population[age - 1][t - 1][sex]["GT30"][d][i-minYear]
                    #Now we're ready to apply survival and migration
                    #Not using age-1 and t-1 (because we already moved the 4 year old t-1 population into the u15 bins)
                    for c in u15CD4States:
                        for d in noARTChildHIVStates:
                            for i in xrange(minYear, t):
                                population[age][t][sex][c][d][i-minYear] = population[age][t][sex][c][d][i-minYear] * sr + population[age][t][sex][c][d][i-minYear] * mr * (1 + sr)/2
                                if population[age][t][sex][c][d][i-minYear] < 0:
                                    population[age][t][sex][c][d][i-minYear] = 0
                        for d in adultARTdurations:
                            for i in xrange(minYear, t):
                                population[age][t][sex][c][d][i-minYear] = population[age][t][sex][c][d][i-minYear] * sr + population[age][t][sex][c][d][i-minYear] * mr * (1 + sr)/2
                                if population[age][t][sex][c][d][i-minYear] < 0:
                                    population[age][t][sex][c][d][i-minYear] = 0
                #Ages 5-14    
                if age <= 14 and age >= 6:
                    for c in u15CD4States:
                        for d in noARTChildHIVStates:
                            for i in xrange(minYear, t):
                                population[age][t][sex][c][d][i-minYear] = population[age-1][t-1][sex][c][d][i-minYear] * sr + population[age-1][t-1][sex][c][d][i-minYear] * mr * (1 + sr)/2
                                if population[age][t][sex][c][d][i-minYear] < 0:
                                    population[age][t][sex][c][d][i-minYear] = 0
                        for d in adultARTdurations:
                            for i in xrange(minYear, t):
                                population[age][t][sex][c][d][i-minYear] = population[age-1][t-1][sex][c][d][i-minYear] * sr + population[age-1][t-1][sex][c][d][i-minYear] * mr * (1 + sr)/2
                                if population[age][t][sex][c][d][i-minYear] < 0:
                                    population[age][t][sex][c][d][i-minYear] = 0


                # Children becoming 15
                if age == 15:
                    #Summing highest CD4 child groups into highest adult group
                    for c in ["GT1000CD4", "750to999CD4","500to749CD4"]:
                        for i in xrange(minYear, t):
                            population[age][t][sex]["GT500CD4"][i-minYear] += sum([population[age - 1][t - 1][sex][c][d][i-minYear] for d in noARTChildHIVStates])
                            for d in adultARTdurations:
                                population[age][t][sex]["ARTGT500CD4"][d][i-minYear] += population[age - 1][t - 1][sex][c][d][i-minYear]
                    #350 - 500 CD4 group remains the same
                    #Split 200-349 child group into 2 adult groups
                    #Split <200 child group into 3 adult groups
                    #Fractions taken from DPPROJ.PAS
                    for i in xrange(minYear, t):
                        population[age][t][sex]["350to500CD4"][i-minYear] = sum([population[age - 1][t - 1][sex]["350to499CD4"][d][i-minYear] for d in noARTChildHIVStates])
                        population[age][t][sex]["200to249CD4"][i-minYear] = 0.333 * sum([population[age - 1][t - 1][sex]["200to349CD4"][d][i-minYear] for d in noARTChildHIVStates])
                        population[age][t][sex]["250to349CD4"][i-minYear] = 0.667 * sum([population[age - 1][t - 1][sex]["200to349CD4"][d][i-minYear] for d in noARTChildHIVStates])
                        population[age][t][sex]["100to199CD4"][i-minYear] = 0.35 * sum([population[age - 1][t - 1][sex]["LT200CD4"][d][i-minYear] for d in noARTChildHIVStates])
                        population[age][t][sex]["50to99CD4"][i-minYear] = 0.21 * sum([population[age - 1][t - 1][sex]["LT200CD4"][d][i-minYear] for d in noARTChildHIVStates])
                        population[age][t][sex]["LT50CD4"][i-minYear] = 0.44 * sum([population[age - 1][t - 1][sex]["LT200CD4"][d][i-minYear] for d in noARTChildHIVStates])
                        #On ART - not sure why we're bumping these into GT12M, just following DPPROJ.PAS
                        population[age][t][sex]["ART350to500CD4"]["GT12Mo"][i-minYear] = sum([population[age - 1][t - 1][sex]["350to499CD4"][d][i-minYear] for d in adultARTdurations])
                        population[age][t][sex]["ART200to249CD4"]["GT12Mo"][i-minYear] = 0.333 * sum([population[age - 1][t - 1][sex]["200to349CD4"][d][i-minYear] for d in adultARTdurations])
                        population[age][t][sex]["ART250to349CD4"]["GT12Mo"][i-minYear] = 0.667 * sum([population[age - 1][t - 1][sex]["200to349CD4"][d][i-minYear] for d in adultARTdurations])
                        population[age][t][sex]["ART100to199CD4"]["GT12Mo"][i-minYear] = 0.35 * sum([population[age - 1][t - 1][sex]["LT200CD4"][d][i-minYear] for d in adultARTdurations])
                        population[age][t][sex]["ART50to99CD4"]["GT12Mo"][i-minYear] = 0.21 * sum([population[age - 1][t - 1][sex]["LT200CD4"][d][i-minYear] for d in adultARTdurations])
                        population[age][t][sex]["ARTLT50CD4"]["GT12Mo"][i-minYear] = 0.44 * sum([population[age - 1][t - 1][sex]["LT200CD4"][d][i-minYear] for d in adultARTdurations])
                        #Apply survival and migration
                        #Not using age-1 and t-1 (because we already moved the 14 year old t-1 population into the adult bins)
                        for c in allCD4states:
                            for i in xrange(minYear, t):
                                if c in noARTCD4states:
                                    population[age][t][sex][c][i-minYear] = population[age][t][sex][c][i-minYear] * sr + population[age][t][sex][c][i-minYear] * mr * (1 + sr) / 2
                                    if population[age][t][sex][c][i-minYear] < 0:
                                        population[age][t][sex][c][i-minYear] = 0
                                else:
                                    for d in adultARTdurations:
                                        population[age][t][sex][c][d][i-minYear] = population[age][t][sex][c][d][i-minYear] * sr + population[age][t][sex][c][d][i-minYear] * mr * (1 + sr) / 2
                                        if population[age][t][sex][c][d][i-minYear] < 0:
                                            population[age][t][sex][c][d][i-minYear] = 0                
                # Infected Adults [16, 80)
                if age in range(16, maxAge):
                    for c in allCD4states:
                        for i in xrange(minYear, t):
                            if c in noARTCD4states:
                                population[age][t][sex][c][i-minYear] = population[age-1][t-1][sex][c][i-minYear] * sr + population[age-1][t-1][sex][c][i-minYear] * mr * (1 + sr) / 2
                                if population[age][t][sex][c][i-minYear] < 0:
                                    population[age][t][sex][c][i-minYear] = 0
                            else:
                                for d in adultARTdurations:
                                    population[age][t][sex][c][d][i-minYear] = population[age-1][t-1][sex][c][d][i-minYear] * sr + population[age-1][t-1][sex][c][d][i-minYear] * mr * (1 + sr) / 2
                                    if population[age][t][sex][c][d][i-minYear] < 0:
                                        population[age][t][sex][c][d][i-minYear] = 0
            # Age group 80
            sr = survivalRates[80][t][sex]
            sr1 = survivalRates[81][t][sex]
            if population[79][t-1][sex]['all'] <= 0:
                mr = 0
            else:
                #capping migration at 80% of pop
                mr = max(((migration[80][t][sex] + migration[79][t][sex])  / 2), (-0.8 * (population[79][t-1][sex]['all'] + population[80][t-1][sex]['all'])))
            deaths[80][t][sex] = population[79][t-1][sex]['all'] * (1 - sr) + population[80][t-1][sex]['all'] * (1 - sr1) + mr * (1 - sr1) / 2

            if (population[79][t-1][sex]['all'] + population[80][t-1][sex]['all']) >= 1:
                mr = mr / (population[79][t-1][sex]['all'] + population[80][t-1][sex]['all'])
            else:
                mr = 0
            for c in allCD4statesNeg:
                if c in noARTCD4statesNeg:
                    if c == 'neg':
                        population[80][t][sex][c] = population[79][t-1][sex][c] * sr + population[79][t-1][sex][c] * mr * (1 + sr) / 2 + population[80][t-1][sex][c] * sr1 + population[80][t-1][sex][c] * mr * (1 + sr1) / 2
                        if population[80][t][sex][c] < 0:
                            population[80][t][sex][c] = 0
                    else:
                        for i in xrange(minYear, t):
                            population[80][t][sex][c][i-minYear] = population[79][t-1][sex][c][i-minYear] * sr + population[79][t-1][sex][c][i-minYear] * mr * (1 + sr) / 2 + population[80][t-1][sex][c][i-minYear] * sr1 + population[80][t-1][sex][c][i-minYear] * mr * (1 + sr1) / 2
                            if population[80][t][sex][c][i-minYear] < 0:
                                population[80][t][sex][c][i-minYear] = 0
                else:
                    for d in adultARTdurations:
                        for i in xrange(minYear, t):
                            population[80][t][sex][c][d][i-minYear] = population[79][t-1][sex][c][d][i-minYear] * sr + population[79][t-1][sex][c][d][i-minYear] * mr * (1 + sr) / 2 + population[80][t-1][sex][c][d][i-minYear] * sr1 + population[80][t-1][sex][c][d][i-minYear] * mr * (1 + sr1) / 2                         
                            if population[80][t][sex][c][d][i-minYear] < 0:
                                population[80][t][sex][c][d][i-minYear] = 0

        numFemales = {}
        for age in xrange(15, 50):
            numFemales[age] = 0
            for h in allCD4statesNeg:
                if h in noARTCD4statesNeg:
                    if h == 'neg':
                        numFemales[age] += population[age][t]['female'][h] + population[age][t-1]['female'][h]
                    else:
                        for i in xrange(minYear, t):
                            numFemales[age] += population[age][t]['female'][h][i-minYear] + population[age][t-1]['female'][h][i-minYear]                            
                else:
                    for d in adultARTdurations:
                        for i in xrange(minYear, t):
                            numFemales[age] += population[age][t]['female'][h][d][i-minYear] + population[age][t-1]['female'][h][d][i-minYear]
        # Calculate the number of births
        allAgeBirths = {'male': 0, 'female': 0}
        for age in xrange(15, 50):
            births[age][t] = {}
            a5 = age - age % 5
            births[age][t]['total'] = TFR[t] * (numFemales[age]) / 2 * (ASFRbyAge[a5][t-minYear] / np.sum(ASFRbyYear[t])) / 5
            births[age][t]['female'] = births[age][t]['total'] * SRB[t]
            births[age][t]['male'] = births[age][t]['total'] * (1 - SRB[t])
            allAgeBirths['male'] += births[age][t]['male']
            allAgeBirths['female'] += births[age][t]['female']

        for sex in sexes:
            sr = survivalRates[0][t][sex]
            if allAgeBirths[sex] >= 1:
                mr = min(migration[0][t][sex] / 2 / allAgeBirths[sex], 0.8)
            else:
                mr = 0

            tempDeaths = allAgeBirths[sex] * (1 - sr) + allAgeBirths[sex] * mr * (2 * (1 - sr)) / 3
            deaths[0][t][sex] = tempDeaths
            population[0][t][sex]['neg'] = allAgeBirths[sex] * sr + allAgeBirths[sex] * mr * (1 + 2 * sr) / 3

        tempPop0_1 = population[0][t]['male']['neg'] + population[0][t]['female']['neg']

        # Push into "all" category
        updateAllStateTotal(t)
        newlyNeedingART = 0
        tempDeaths = 0
        for c in noARTCD4states:
            for sex in sexes:
                eligibleSpecialPops[c][sex] = 0

        # Calculate Adult ART coverage in current and previous two years
        allInterventionCoverage[t]['adult'][sex]['ART']['eligible'] = 0
        for sex in sexes:
            for age in xrange(childMaxAge + 1, maxAge + 1):
                ARTneed = 0
                for c in noARTCD4states:
                     ## Adding next eligibility group up 2/21/18
                    if c == 'LT50CD4':
                        lowerEligGroup = 'LT50CD4'
                    else:
                        lowerEligGroup = noARTCD4states[noARTCD4states.index(c) -1]
                    if CD4lowerLimits[lowerEligGroup] < adultARTeligibility[t - minYear]:
                        for i in xrange(minYear, t):
                            ARTneed += population[age][t][sex][c][i-minYear]
                    else:
                        for p in eligiblePops:
                            if p != 'pregnant_women' and eligiblePopYears[p]-1 <= t:
                                if not ((p == 'MSM' and sex == 'female') or (p == 'FSW' and sex == 'male')):
                                    for i in xrange(minYear, t):
                                        ARTneed += population[age][t][sex][c][i-minYear] * eligiblePopPercents[p]
                                        eligibleSpecialPops[c][sex] += population[age][t][sex][c][i-minYear] * eligiblePopPercents[p]
                for c in ARTCD4states:
                    for d in adultARTdurations:
                        for i in xrange(minYear, t):
                            ARTneed += population[age][t][sex][c][d][i-minYear]

                # Get the number of eligible pregnant women (DPPROJA.PAS getEligiblePregnantWomen)
                # Get HIV+ women not on ART (PW)
                if 'pregnant_women' in eligiblePops:
                    if sex == 'female' and age in xrange(15, 50) and eligiblePopYears['pregnant_women'] <= t:
                        pregWomenNeed = getEligiblePregnantWomen(age, t)
                        ARTneed += pregWomenNeed
                needForART[age][t][sex] = ARTneed
                need15plus[t][sex] += ARTneed

                allInterventionCoverage[t]['adult'][sex]['ART']['eligible'] += ARTneed

            # Calculate ART-eligible adults by state, time, age, and sex (DPPROJA.PAS 859)
            # Get ART-eligible adults by time and sex for t and up to t-2
            if usePredCoverage == False:
                eligibleAdults[t-minYear][sex] = 0
                eligibleAdults[t-minYear-1][sex] = 0
                try:
                    eligibleAdults[t-minYear-2][sex] = 0
                except:
                    pass
                for age in xrange(15, maxAge+1):
                    for c in noARTCD4states:
                        if CD4lowerLimits[c] < adultARTeligibility[t-minYear]:
                            for i in xrange(minYear, t):
                                eligibleAdults[t-minYear][sex] += population[age][t][sex][c][i-minYear]
                                eligibleAdults[t-minYear-1][sex] += population[age][t-1][sex][c][i-minYear]
                                try:
                                    eligibleAdults[t-minYear-2][sex] += population[age][t-2][sex][c][i-minYear]
                                except:
                                    pass
                        else:
                            if sex == 'female' and age in xrange(15, 50):
                                eligibleAdults[t-minYear][sex] += getEligiblePregnantWomen(age, t)
                                eligibleAdults[t-minYear-1][sex] += getEligiblePregnantWomen(age, t-1)
                                try:
                                    eligibleAdults[t-minYear-2][sex] += getEligiblePregnantWomen(age, t-2)
                                except:
                                    pass
                # Use ART coverage to get total adult patients in a year
                try:
                    if adultARTCoverageType[t-minYear-2] == 'percent':
                        twoPrevYearsART[sex] = adultARTCoverage[sex][t-minYear-2] * need15plus[t-2][sex]
                    else:   
                        twoPrevYearsART[sex] = adultARTCoverage[sex][t-minYear-2]
                except:
                    twoPrevYearsART[sex] = 0
                if adultARTCoverageType[t-minYear-1] == 'percent':
                    prevYearART[sex] = adultARTCoverage[sex][t-minYear-1] * need15plus[t-1][sex] / 100
                else:
                    prevYearART[sex] = adultARTCoverage[sex][t-minYear-1]
            if adultARTCoverageType[t-minYear] == 'percent':
                currentYearART[sex] = adultARTCoverage[sex][t-minYear] * need15plus[t][sex] / 100
            else:
                currentYearART[sex] = adultARTCoverage[sex][t-minYear] * sum([population[age][t][sex]["all"] for age in range(15, 81)]) / 100
            sumART = 0
        #capping coverage at 90% of the total population living with HIV - 2/28/17
            plwhiv = sum([sum([sum(population[age][t][sex][c]) for c in noARTCD4states]) for age in range(15,81)])
            plwhiv +=  sum([sum([sum([sum(population[age][t][sex][c][d]) for d in adultARTdurations]) for c in ARTCD4states]) for age in range(15,81)])
            if currentYearART[sex] > (0.9 * plwhiv):
                currentYearART[sex] = 0.9 * plwhiv
        if usePredCoverage == True:    
            popByCD4 = getPopByCD4(population, noARTCD4states)
            ARTCoverageCounts = getPredARTCoverageCounts(popByCD4, predARTCoverage, t, currentYearART, noARTCD4states)

        # Apply ART initiation, HIV/AIDS mortality, and CD4 category progression 10 times per year
        if scaleOnART == True:
            if t < min(inputOnARTScalar.year):
                onARTScalar = inputOnARTScalar.loc[inputOnARTScalar.year == min(inputOnARTScalar.year),'rr'].values
            elif t > max(inputOnARTScalar.year):
                onARTScalar = inputOnARTScalar.loc[inputOnARTScalar.year == max(inputOnARTScalar.year),'rr'].values
            else:
                onARTScalar = inputOnARTScalar.loc[inputOnARTScalar.year == t,'rr'].values
            onARTScalar = np.asscalar(onARTScalar)
        else:
            onARTScalar = 1
        for t1 in xrange(1, 11):
            for sex in sexes:
                if usePredCoverage == False:
                    eligibleAdults[t - minYear][sex] = 0
                    for age in xrange(15, 81):
                            for c in noARTCD4states:
                                if CD4lowerLimits[c] < adultARTeligibility[t-minYear]:
                                    for i in xrange(minYear, t):
                                        eligibleAdults[t - minYear][sex] += population[age][t][sex][c][i-minYear]
                                if sex =='female' and age in xrange(15, 50):
                                    eligibleAdults[t - minYear][sex] += getEligiblePregnantWomen(age, t)

                    # Use ART survivors to get new adult ART patients
                    test_aggregate = {25:0, 50:0}
                    ARTsurvivors = 0
                    for age in xrange(15, maxAge + 1):
                        age10 = ((age - (age - 5) % 10) - 15) / 10
                        if age > 55:
                            age10 = ((55 - (55 -5) % 10) - 15) / 10
                        for c in ARTCD4states:
                            for d in adultARTdurations:
                                alpha = onARTmortality[sex][d][age10][c] * onARTScalar
                                for i in xrange(minYear, t):
                                    ARTsurvivors += population[age][t][sex][c][d][i-minYear] * (1 - alpha / timeStep)
                                # ARTsurvivors += sum(population[age][t][sex][c][d]) * (1 - alpha / timeStep)
                                if age > 25:
                                    test_aggregate[50] += sum(population[age][t][sex][c][d])
                                if age <= 25:
                                    test_aggregate[25] += sum(population[age][t][sex][c][d])

                    if adultARTCoverageType[t-minYear] == 'percent':
                        neededART = ARTsurvivors + (currentYearART[sex] - ARTsurvivors) / timeStep * t1
                    else:
                        if t1 < math.trunc(timeStep / 2):
                            neededART = (twoPrevYearsART[sex] + (prevYearART[sex] - twoPrevYearsART[sex]) / timeStep
                                * (t1 + (timeStep / 2)))
                        else:
                            neededART = (prevYearART[sex] + (currentYearART[sex] - prevYearART[sex]) / timeStep
                                * (t1 - (timeStep / 2)))

                    newART = neededART - ARTsurvivors
                    if newART < 0:
                        newART = 0

                    newART = min(newART, eligibleAdults[t-minYear][sex])
                    prop1 = {}
                    prop2 = {}

                    for c in noARTCD4states:
                        eligibleAdultsCD4[c][sex] = 0
                        prop1[c] = 0
                        prop2[c] = 0
                        if CD4lowerLimits[c] < adultARTeligibility[t-minYear]:
                            for age in xrange(15, maxAge + 1):
                                eligibleAdultsCD4[c][sex] += sum(population[age][t][sex][c])
                        else:
                            eligibleAdultsCD4[c][sex] += eligibleSpecialPops[c][sex]
                        # IF EACH CD4 CATEGORY GETS THE SAME WEIGHT IN NEW ART:
                        # Get the proportion of each category by sex beginning treatment
                        for i in xrange(4):
                            eligByAge[i][c][sex] = 0

                        if eligibleAdults[t-minYear][sex] > 0:
                                prop1[c] = newART / eligibleAdults[t-minYear][sex]
                        else:
                            prop1[c] = 0

                        for age in xrange(15, maxAge+1):
                            age10 = ((age - (age - 5) % 10) - 15) / 10
                            if age > 45:
                                age10 = ((45 - (45 -5) % 10) - 15) / 10
                            for i in xrange(minYear, t):
                                eligByAge[age10][c][sex] += population[age][t][sex][c][i-minYear]

                        # Calculate all-age noART deaths for each CD4 category
                        sum1 = 0
                        sum2 = 0
                        for a1 in xrange(4):
                            mu = noARTmortality[sex][a1][c]
                            sum1 += mu * eligByAge[a1][c][sex]
                            sum2 += eligByAge[a1][c][sex]

                        # Calculate mortality for each CD4 category
                        if sum2 > 0:
                            mortRate[c] = sum1/sum2
                        else:
                            mortRate[c] = 0

                    # Get eligible adults by age group, CD4, and sex
                    sum3 = 0


                    for i in xrange(len(noARTCD4states)):
                        tempSum = 0
                        for c1 in noARTCD4states[i:]:
                            tempSum += eligibleAdultsCD4[c1][sex] * mortRate[c1]
                        if tempSum > 0:
                            newPatients[noARTCD4states[i]][sex] = newART * eligibleAdultsCD4[noARTCD4states[i]][sex] * mortRate[noARTCD4states[i]] / tempSum
                        else:
                            newPatients[noARTCD4states[i]][sex] = 0
                        newPatients[noARTCD4states[i]][sex] = min(newPatients[noARTCD4states[i]][sex], eligibleAdultsCD4[noARTCD4states[i]][sex])
                        sum3 += newPatients[noARTCD4states[i]][sex]
                        newART -= newPatients[noARTCD4states[i]][sex]

                    # Calculate weighted average of # eligible and mortality
                    for c in noARTCD4states:
                        if eligibleAdultsCD4[c][sex] > 0:
                            prop2[c] = newPatients[c][sex] / eligibleAdultsCD4[c][sex]
                        else:
                            prop2[c] = 0

                        # Average the two proportions to get the distribution of new patients
                        newPatients[c][sex] = (prop1[c] + prop2[c]) / 2 * eligibleAdultsCD4[c][sex]

                for age in xrange(childMaxAge + 1, maxAge + 1):
                    age5 = age - age % 5
                    age5_2 = (age - 15) - (age - 15) % 10 + 15
                    age10 = ((age - (age - 5) % 10) - 15) / 10
                    if age > 45:
                        age5_2 = 45 - 45 % 5
                        age10 = ((45 - (45 - 5) % 10) - 15) / 10
                    age10_2 = ((age - (age - 5) % 10) - 15) / 10
                    if age > 55:
                        age10_2 = ((55 - (55 - 5) % 10) - 15) / 10
                    GT12MoDeaths = 0
                    tmp_startart = 0
                    tmp_noARTpop = 0
                    tmp_total_share = 0
                    for c in reversed(xrange(len(noARTCD4states))):
                        tmp_new_art = 0
                        tmp_noARTpop_2 = 0
         
                        age10_num = (10*(age10 + 1) + 5)
                        if (sum([sum([sum(population[a10][t][sex][ARTCD4states[c]][d]) for a10 in range(age10_num, age10_num + 10)]) for d in adultARTdurations]) + sum([sum(population[a10][t][sex][noARTCD4states[c]]) for a10 in range(age10_num, age10_num + 10)])) > 0:
                            if age <= 45:
                                lastTenthCov = sum([sum([sum(population[a10][t][sex][ARTCD4states[c]][d]) for a10 in range(age10_num, age10_num + 10)]) for d in adultARTdurations]) / (sum([sum([sum(population[a10][t][sex][ARTCD4states[c]][d]) for a10 in range(age10_num, age10_num + 10)]) for d in adultARTdurations]) + sum([sum(population[a10][t][sex][noARTCD4states[c]]) for a10 in range(age10_num, age10_num + 10)]))
                            else:
                                lastTenthCov = sum([sum([sum(population[a10][t][sex][ARTCD4states[c]][d]) for a10 in range(age10_num, 81)]) for d in adultARTdurations]) / (sum([sum([sum(population[a10][t][sex][ARTCD4states[c]][d]) for a10 in range(age10_num, 81)]) for d in adultARTdurations]) + sum([sum(population[a10][t][sex][noARTCD4states[c]]) for a10 in range(age10_num, 81)]))

                        else:
                            lastTenthCov = 0
                        if usePredCoverage == True:
                            cd4 = noARTCD4states[c]
                            targetCoverage = ARTCoverageCounts[sex][age][cd4]
                            survivors = getARTSurvivors(adultARTdurations,onARTmortality,sex,t,timeStep, population, age, cd4, onARTScalar)
                        for i in xrange(minYear, t):
                            startART = 0
                            if sum(population[age][t][sex][noARTCD4states[c]]) > 0:
                                tmp_inf_share = population[age][t][sex][noARTCD4states[c]][i-minYear] / sum(population[age][t][sex][noARTCD4states[c]])
                            else:
                                tmp_inf_share = 0
                            if usePredCoverage == True:
                                startART = max(0.0, tmp_inf_share * (targetCoverage - survivors)/(timeStep - (t1 - 1)))
                            else:
                                if eligibleAdultsCD4[noARTCD4states[c]][sex] <= 0:
                                    startART = 0
                                else:
                                    if CD4lowerLimits[noARTCD4states[c]] < adultARTeligibility[t-minYear]:
                                        # SPECIAL POPULATIONS
                                        startART = tmp_inf_share * min(sum(population[age][t][sex][noARTCD4states[c]]), newPatients[noARTCD4states[c]][sex] * sum(population[age][t][sex][noARTCD4states[c]]) / eligibleAdultsCD4[noARTCD4states[c]][sex])
                                        tmp_noARTpop += population[age][t][sex][noARTCD4states[c]][i-minYear]
                                        tmp_noARTpop_2 += population[age][t][sex][noARTCD4states[c]][i-minYear]
                                tmp_startart += startART
                                if c == 0:
                                    tmp_total_share += tmp_inf_share
                                tmp_new_art += startART

                            sumART += startART
                            population[age][t][sex][noARTCD4states[c]][i-minYear] -= startART
                            if population[age][t][sex][noARTCD4states[c]][i-minYear] < 0:
                                population[age][t][sex][noARTCD4states[c]][i-minYear] = 0
                            # Calculate noART entrants to and exits from groups
                            if noARTCD4states[c] == 'GT500CD4':
                                # This will be incidence
                                entrants[noARTCD4states[c]][1][i-minYear] = 0
                            else:
                                # Use lambda to calculate entrants from one noART HIV+ group to another
                                entrants[noARTCD4states[c]][1][i-minYear] = population[age][t][sex][noARTCD4states[c+1]][i-minYear] * progressionParameters[age5_2][sex][noARTCD4states[c+1]]
                            mu = noARTmortality[sex][age10][noARTCD4states[c]]
                            # Caculate the total number of exits from a group
                            exits[noARTCD4states[c]][1][i-minYear] = (population[age][t][sex][noARTCD4states[c]][i-minYear] *
                                (progressionParameters[age5_2][sex][noARTCD4states[c]] + mu)) 
                            # exits[noARTCD4states[c]][1] = (population[age][t][sex][noARTCD4states[c]] *
                            #   (lambdaCoeff(age, noARTCD4states[c]) + mu)) + startART

                            temp = min(mu * population[age][t][sex][noARTCD4states[c]][i-minYear], population[age][t][sex][noARTCD4states[c]][i-minYear])
                            AIDSdeaths[age][t][sex][i-minYear] += max(0, temp)
                            tempDeaths += temp

                            AIDSdeathsCD4[age5][t][sex][noARTCD4states[c]] += max(0, temp)

                            # Calculate new need for ART
                            if CD4lowerLimits[noARTCD4states[c]] == adultARTeligibility[t-minYear]:
                                newlyNeedingART += population[age][t][sex][noARTCD4states[c]][i-minYear] * progressionParameters[age5_2][sex][noARTCD4states[c]]
                            # Calculate movement in the onART categories
                            for d in adultARTdurations:
                                alpha = onARTmortality[sex][d][age10_2][ARTCD4states[c]] * onARTScalar
                                if d == 'LT6Mo':
                                    entrants[ARTCD4states[c]][d][i-minYear] = startART
                                    exits[ARTCD4states[c]][d][i-minYear] = population[age][t][sex][ARTCD4states[c]][d][i-minYear] * alpha / timeStep + population[age][t][sex][ARTCD4states[c]][d][i-minYear] * (12 / 6) / timeStep
                                elif d == '6to12Mo':
                                    entrants[ARTCD4states[c]][d][i-minYear] = population[age][t][sex][ARTCD4states[c]]['LT6Mo'][i-minYear] * (12 / 6) / timeStep
                                    exits[ARTCD4states[c]][d][i-minYear] = population[age][t][sex][ARTCD4states[c]][d][i-minYear] * alpha / timeStep + population[age][t][sex][ARTCD4states[c]][d][i-minYear] * (12 / 6) / timeStep
                                elif d == 'GT12Mo':
                                    entrants[ARTCD4states[c]][d][i-minYear] = population[age][t][sex][ARTCD4states[c]]['6to12Mo'][i-minYear] * (12 / 6) / timeStep
                                    exits[ARTCD4states[c]][d][i-minYear] = population[age][t][sex][ARTCD4states[c]][d][i-minYear] * alpha /timeStep
                                    GT12MoDeaths += alpha * population[age][t][sex][ARTCD4states[c]][d][i-minYear] / timeStep
                                temp = min(alpha * population[age][t][sex][ARTCD4states[c]][d][i-minYear] / timeStep, population[age][t][sex][ARTCD4states[c]][d][i-minYear])
                                AIDSdeaths[age][t][sex][i-minYear] += max(0, temp)
                                AIDSdeathsCD4[age5][t][sex][ARTCD4states[c]][d] += max(0, temp)

                    for c in xrange(len(noARTCD4states)):
                        # Add entrants and remove exits
                        for i in xrange(minYear, t):
                            population[age][t][sex][noARTCD4states[c]][i-minYear] = max(0, population[age][t][sex][noARTCD4states[c]][i-minYear] + entrants[noARTCD4states[c]][1][i-minYear] - exits[noARTCD4states[c]][1][i-minYear])
                            for d in adultARTdurations:
                                population[age][t][sex][ARTCD4states[c]][d][i-minYear] = max(0, population[age][t][sex][ARTCD4states[c]][d][i-minYear] + entrants[ARTCD4states[c]][d][i-minYear] - exits[ARTCD4states[c]][d][i-minYear])

        updateAllStateTotal(t)                  
        lastAge = 49

        susceptiblePop = {}
        for sex in sexes:
            susceptiblePop[sex] = {}
            if output_type == 'five_year':
                for age in xrange(0, 80, 5):
                    susceptiblePop[sex][age] = 0
                susceptiblePop[sex][80] = 0
            elif output_type == 'single_year':
                for age in xrange(0, 81):
                    susceptiblePop[sex][age] = 0


        for sex in sexes:
            for age in xrange(0, 81):
                if output_type == 'five_year':
                    susceptiblePop[sex][(age - (age % 5))] += population[age][t][sex]['neg']
                elif output_type == 'single_year':
                    susceptiblePop[sex][age] += population[age][t][sex]['neg']

        adults = {}
        adultHIV = {}
        neededHIV = {}
        prevRatio = {}
        for sex in sexes:
            if t == minYear + 1:
                incidenceAdjFactor = 1
            adults[sex] = 0
            adultHIV[sex] = 0

            for age in xrange(15, lastAge + 1):
                adults[sex] += population[age][t][sex]['all']
                for c in allCD4states:
                    if c in noARTCD4states:
                        for i in xrange(minYear, t):
                            adultHIV[sex] += population[age][t][sex][c][i-minYear]
                    else:
                        for d in adultARTdurations:
                            for i in xrange(minYear, t):
                                adultHIV[sex] += population[age][t][sex][c][d][i-minYear]

        adults['both'] = adults['male'] + adults['female']
        adultHIV['both'] = adultHIV['male'] + adultHIV['female']

        neededHIV['both'] = adults['both'] * EPPdata[t]['prev'] / 100 - adultHIV['both']


        if neededHIV['both'] > 0 and not (EPPdata[t]['inc'] == 0 and EPPdata[t]['prev'] > 0):
            incidenceAdjFactor = (neededHIV['both'] / (adults['both'] - adultHIV['both']) * 100) / EPPdata[t]['inc']
        elif neededHIV['both'] <= 0 or (EPPdata[t]['inc'] == 0 and EPPdata[t]['prev'] > 0):
            incidenceAdjFactor = 1

        if t == 2012:
            incidenceAdjFactor2012 = float(incidenceAdjFactor)

        if t > 2012:
            incidenceAdjFactor = incidenceAdjFactor2012

        prevYearAdults = {}
        prevYearAdultHIV = {}
        for sex in sexes:
            prevYearAdultHIV[sex] = 0
            prevYearAdults[sex] = 0
            for age in xrange(15, lastAge + 1):
                prevYearAdults[sex] += population[age][t-1][sex]['all']
                for c in allCD4states:
                    if c in noARTCD4states:
                        for i in xrange(minYear, t):
                            prevYearAdultHIV[sex] += population[age][t-1][sex][c][i-minYear]
                    else:
                        for d in adultARTdurations:
                            for i in xrange(minYear, t):
                                prevYearAdultHIV[sex] += population[age][t-1][sex][c][d][i-minYear]
        newHIV = {}

        if (prevYearAdults['female'] - prevYearAdultHIV['female'] + prevYearAdults['male'] - prevYearAdults['female']) == 0:
            temp  = 0
        else:
            temp = EPPdata[t]['inc'] / 100
        if inc_adj_config == 0:
            incidenceAdjFactor = 1

        # print 'ISR: %d' % incSexRatio[t-minYear]
        # print 'prev: %d' % (prevYearAdults['female'] - prevYearAdultHIV['female'] + (prevYearAdults['male'] - prevYearAdultHIV['male']))
        if ((prevYearAdults['female'] - prevYearAdultHIV['female'] + (prevYearAdults['male'] - prevYearAdultHIV['male'])) > 0):
            temp = (temp * incidenceAdjFactor *
                (prevYearAdults['female'] - prevYearAdultHIV['female'] +
                    prevYearAdults['male'] - prevYearAdultHIV['male']) /
                (prevYearAdults['female'] - prevYearAdultHIV['female'] +
                    (prevYearAdults['male'] - prevYearAdultHIV['male']) / incSexRatio[t - minYear]))
        else:
            temp = 0

        newHIV['female'] = max(0, temp * (prevYearAdults['female'] - prevYearAdultHIV['female']))

        temp = temp / incSexRatio[t-minYear]

        newHIV['male'] = max(0, temp * (prevYearAdults['male'] - prevYearAdultHIV['male']))

        newHIV['both'] = newHIV['female'] + newHIV['male']

        newHIVbyAge5 = {}
        newHIVbyAge = {}
        popBy5 = {}

        for sex in sexes:
            newHIVbyAge[sex] = {}
            newHIVbyAge5[sex] = {}
            popBy5[sex] = {}
            for age in xrange(0, 15):
                newHIVbyAge[sex][age] = 0

            #If we're in stage 1, we need to distribute HIV incidence by age and sex
            if stage == "stage_1":
                tempSum = 0
                pop5 = []
                ageRange = range(15,80,5)
                ageRange.append(80)
                for a5 in xrange(15, 50, 5):
                    pop5.append(0)
                    for age in xrange(a5, a5 + 5):
                        tempSum += population[age][t][sex]['neg'] * incAgeDist[sex][0][a5]
                for a5 in ageRange:
                    pop5.append(0)
                    if a5 < 80:
                        for age in xrange(a5, a5 + 5):
                            pop5[a5/5 - 3] += population[age][t][sex]['neg']
                    else:
                        pop5[a5/5 - 3] += population[a5][t][sex]['neg']
                if tempSum != 0:
                    adjFactor = newHIV[sex] / tempSum
                else:
                    adjFactor = 0
                hiv = []
                for age in ageRange:
                    hiv.append(adjFactor * incAgeDist[sex][0][age] * pop5[age/5 - 3])


                tempInterp = beers.BeersInterpolateGroups(hiv)
                for i in range(len(tempInterp)):
                    tempInterp[i] = max(0, tempInterp[i])

            for age in xrange(15, maxAge + 1):
                #If we're in stage 2, newHIVbyAge is determined by incidence from EPP and negative population
                #March 2018 - using GBD16 results as stage 1 group 2 prior
                #if stage == 'stage_2' or group[0] == '2':
                if stage == 'stage_2' or group[0] == '2':
                    newHIVbyAge[sex][age] = incByAgeSex[t][sex][age] * population[age][t][sex]['neg']

                else:
                    newHIVbyAge[sex][age] = min(tempInterp[age-15], population[age][t][sex]['neg'])

        if stage == "stage_2" and parent == "IND":
            for sex in sexes:
                pred_inc = 0
                pop_neg = 0
                for age in range(15, 50):
                    pred_inc += newHIVbyAge[sex][age]
                    pop_neg = population[age][t][sex]['neg']
                epp_inc = newHIV[sex]
                if pred_inc > 0:
                    scalar = epp_inc / pred_inc
                else:
                    scalar = 0
                for age in range(15, 50):                
                    newHIVbyAge[sex][age] = newHIVbyAge[sex][age] * scalar

        for sex in sexes:            
            for age in xrange(15, maxAge + 1):
                if age > 45:
                    age10 = ((45 - (45 - 5) % 10) - 15) / 10
                else:
                    age10 = ((age - (age - 5) % 10) - 15) / 10

                for c in noARTCD4states:
                    population[age][t][sex][c][t-minYear] += min(population[age][t][sex]['neg'], max(0, newHIVbyAge[sex][age] * initCD4dist[age10][c][sex] / 100))

                population[age][t][sex]['neg'] -= min(population[age][t][sex]['neg'], max(0, newHIVbyAge[sex][age]))

            for age5 in xrange(15, maxAge, 5):
                newHIVbyAge5[sex][age5] = 0
                popBy5[sex][age5] = 0
                for a in xrange(age5, age5 + 5):
                    newHIVbyAge5[sex][age5] += newHIVbyAge[sex][a]
                    popBy5[sex][age5] += population[a][t][sex]['all']


        tempBirths = 0
        tempBirths15to24 = 0
        needPMTCT = 0

        # Calculate the nubmer of births to HIV+ women
        for age5 in xrange(15, 49, 5):
            HIVwomen = 0 
            negWomen = 0
            ARTthisYear = 0
            ARTprevYear = 0
            age5c = age5/5 - 3
            for age in xrange(age5, age5 + 5):
                negWomen += (population[age][t]['female']['neg'] + population[age][t-1]['female']['neg']) / 2
                for c in allCD4states:
                    if c in noARTCD4states:
                        HIVwomen += (sum(population[age][t]['female'][c]) + sum(population[age][t-1]['female'][c])) / 2
                    else:
                        for d in adultARTdurations:
                            HIVwomen += (sum(population[age][t]['female'][c][d]) + sum(population[age][t-1]['female'][c][d])) / 2
                            ARTthisYear += sum(population[age][t]['female'][c][d])
                            ARTprevYear += sum(population[age][t-1]['female'][c][d])

            if HIVwomen + negWomen > 0:
                prev1 = (HIVwomen - (ARTthisYear + ARTprevYear) / 2) / (HIVwomen + negWomen)
                prev2 = (HIVwomen) / (HIVwomen + negWomen)
            else:
                prev1 = 0.0
                prev2 = 0.0
            noARTbirths = ((HIVwomen - (ARTthisYear + ARTprevYear)/2) * (TFR[t]/(prev1 + (1-prev1)/TFRreduction[age5c]))
                    * ((ASFRbyAge[age5][t-minYear] / np.sum(ASFRbyYear[t])) / 5))
            ARTbirths = ((ARTthisYear + ARTprevYear)/2 * TFR[t]) *  (ASFRbyAge[age5][t-minYear] / np.sum(ASFRbyYear[t])) / 5
            birthsToPosMothers['female'][age5] = noARTbirths + ARTbirths
            tempBirths += (HIVwomen * TFR[t] * TFRreduction[age5c] / (TFRreduction[age5c] * prev1 + (1 - prev1))
                * (ASFRbyAge[age5][t-minYear] / np.sum(ASFRbyYear[t])) / 5)
            # tempBirths += noARTbirths + ARTbirths
            birthsToAllMothers['female'][age5] = ((HIVwomen+negWomen) * TFR[t]) *  (ASFRbyAge[age5][t-minYear] / np.sum(ASFRbyYear[t])) / 5
            if age5 < 25:
                tempBirths15to24 += (HIVwomen * TFR[t] * TFRreduction[age5c] / (TFRreduction[age5c] * prev1 + (1 - prev1))
                    * (ASFRbyAge[age5][t-minYear] / np.sum(ASFRbyYear[t])) / 5)

        abortion = 0
        tempBirths -= abortion
        if tempBirths < 0:
            tempBirths = 0
        needPMTCT = tempBirths

        tempBirths15to50 = tempBirths

        treatPercent = {}

        # Apply PMTCT
        percent_treatments = [p for p in prenatalProph.keys() if PMTCTtype['prenat'][p][t-minYear] == 'percent']
        num_treatments = [p for p in prenatalProph.keys() if PMTCTtype['prenat'][p][t-minYear] == 'num']

        tmp_denom = 0
        for p in percent_treatments:
            tmp_denom += prenatalProph[p][t-minYear] * needPMTCT / 100
        for p in num_treatments:
            tmp_denom += prenatalProph[p][t-minYear]

        denom = max(tmp_denom, needPMTCT)

        for p in PMTCTtreatmentOptions:
            treatPercent[p] = 0

        for p in num_treatments:
            if denom > 0:
                treatPercent[p] = prenatalProph[p][t-minYear] / denom
            else:
                treatPercent[p] = 0
        for p in percent_treatments:
            treatPercent[p] = prenatalProph[p][t-minYear] / 100

        tempSum = 0
        for p in treatPercent:
            if p != 'noProph':
                treatPercent[p] = min(1, treatPercent[p])
                tempSum += treatPercent[p]
        treatPercent['noProph'] = 1 - tempSum

        temppercent = dict(treatPercent)

        tempSum1 = 0
        tempSum2 = 0
        tempSum3 = 0
        for age in xrange(15, 50):
            for c in noARTCD4states[:3]:
                tempSum1 += sum(population[age][t]['female'][c])
            tempSum2 += sum(population[age][t]['female']['200to249CD4']) + sum(population[age][t]['female']['250to349CD4'])
            tempSum3 += sum(population[age][t]['female']['350to500CD4']) + sum(population[age][t]['female']['GT500CD4'])

        if tempSum1 + tempSum2 + tempSum3 > 0:
            propLT200 = tempSum1 / (tempSum1 + tempSum2 + tempSum3)
            prop200to350 = tempSum2 / (tempSum1 + tempSum2 + tempSum3)
            propGE350 = tempSum3 / (tempSum1 + tempSum2 + tempSum3)
        else:
            propLT200 = 0
            prop200to350 = 1
            propGE350 = 0

        propLT350 = propLT200 + prop200to350

        if treatPercent['optionA'] + treatPercent['optionB'] > propGE350:
            if propGE350 <= 0:
                excessRatio = 0
            else:
                excessRatio = (treatPercent['optionA'] + treatPercent['optionB']) / propGE350 - 1
            optionATransRate = MTCtransRates['optionA']['perinatal'] * (1 + excessRatio)
            optionBTransRate = MTCtransRates['optionB']['perinatal'] * (1 + excessRatio)
        else:
            optionATransRate = MTCtransRates['optionA']['perinatal']
            optionBTransRate = MTCtransRates['optionB']['perinatal']

        PTR = 0
        percentInProgram = 0
        for p in treatPercent:
            if p not in ['optionA', 'optionB', 'noProph']:
                PTR += treatPercent[p] * MTCtransRates[p]['perinatal']
                percentInProgram += treatPercent[p]
        PTR += treatPercent['optionA'] * optionATransRate + treatPercent['optionB'] * optionBTransRate
        percentInProgram += treatPercent['optionA'] + treatPercent['optionB']

        tempSum1 = 0
        tempSum2 = 0
        for age5 in xrange(15, 49, 5):
            tempSum1 += newHIVbyAge5['female'][age5]
        for age in xrange(15, 50):
            for c in noARTCD4states:
                tempSum2 += sum(population[age][t]['female'][c])
        if tempSum2 > 0:
            propIncidentInfections = tempSum1 / tempSum2
        else:
            propIncidentInfections = 0
        PTR += propIncidentInfections * treatPercent['noProph'] * MTCtransRates['IncidentInf']['perinatal']
        propGE350 = max(propGE350 - propIncidentInfections, 0)

        PTR += treatPercent['noProph'] * (1 - propIncidentInfections) * (propLT200 * MTCtransRates['LT200']['perinatal']
            + prop200to350 * MTCtransRates['200to350']['perinatal'] + propGE350 * MTCtransRates['GT350']['perinatal'])

        perinatalTransmission.append(PTR)
        
        #HIVbirths is birth prevalence (perinatal transmission)
        HIVbirths = max(0, tempBirths * PTR)
        #capHIV is number of births to HIV + mothers
        capHIV = tempBirths
        hivBirthPrevOut = {'female':0, 'male':0}

        if stage == 'stage_2' and parent != 'IND':
            newChildHIV['male'][0] += min(HIVbirths * (1 - SRB[t]) * childIncAdj[t][0]['male'], (capHIV * (1 - SRB[t])))
            newChildHIV['female'][0] += min(HIVbirths * SRB[t]  * childIncAdj[t][0]['female'], (capHIV * SRB[t]))
            HIVbirths = newChildHIV['male'][0] + newChildHIV['female'][0]
            capHIV = max(capHIV - HIVbirths, 0)
        else:
            newChildHIV['male'][0] += HIVbirths * (1 - SRB[t])
            newChildHIV['female'][0] += HIVbirths * SRB[t]
        hivBirthPrevOut['female'] = newChildHIV['female'][0]
        hivBirthPrevOut['male'] = newChildHIV['male'][0]
        print(HIVbirths)
        print(hivBirthPrevOut)

        sixWeekMTCT = PTR

        # BF Transmission

        percent_treatments = [p for p in prenatalProph.keys() if PMTCTtype['prenat'][p][t-minYear] == 'percent']
        num_treatments = [p for p in prenatalProph.keys() if PMTCTtype['prenat'][p][t-minYear] == 'num']
        percent_treatments_BF = [p for p in postnatalProph.keys() if PMTCTtype['postnat'][p][t-minYear] == 'percent']
        num_treatments_BF = [p for p in postnatalProph.keys() if PMTCTtype['postnat'][p][t-minYear] == 'num']

        tmp_denom = 0
        for p in percent_treatments:
            tmp_denom += prenatalProph[p][t-minYear] * needPMTCT / 100
        for p in num_treatments:
            tmp_denom += prenatalProph[p][t-minYear]

        denom = max(tmp_denom, needPMTCT)
        for p in percent_treatments:
            allInterventionCoverage[t]['adult']['female'][p]['coverage'] += prenatalProph[p][t-minYear] * denom / 100
        for p in num_treatments:
            allInterventionCoverage[t]['adult']['female'][p]['coverage'] += prenatalProph[p][t-minYear]

        for p in percent_treatments_BF:
            p_ext = p + '_BF'
            if p in ['optionA', 'optionB']:
                allInterventionCoverage[t]['adult']['female'][p_ext]['coverage'] += postnatalProph[p][t-minYear] * denom / 100
        for p in num_treatments_BF:
            p_ext = p + '_BF'
            if p in ['optionA', 'optionB']:
                allInterventionCoverage[t]['adult']['female'][p_ext]['coverage'] += postnatalProph[p][t-minYear]

        for p in allInterventionCoverage[t]['adult']['female']:
            if p not in ['ART', 'CTX']:
                allInterventionCoverage[t]['adult']['female'][p]['eligible'] = denom

        if PMTCTtype['prenat']['tripleARTdurPreg'][t-minYear] == 'percent':
            tripleARTdurPregNum = prenatalProph['tripleARTdurPreg'][t-minYear] * denom / 100
        else:
            tripleARTdurPregNum = prenatalProph['tripleARTdurPreg'][t-minYear]
        if PMTCTtype['prenat']['tripleARTbefPreg'][t-minYear] == 'percent':
            tripleARTbefPregNum = prenatalProph['tripleARTbefPreg'][t-minYear] * denom / 100
        else:
            tripleARTbefPregNum = prenatalProph['tripleARTbefPreg'][t-minYear]

        treatPercent['noProph'] = 1.0
        for p in PMTCTtreatmentOptions:
            treatPercent[p] = 0
        treatPercent['optionA_BF'] = 0
        treatPercent['optionB_BF'] = 0

        if denom > 0:
            if (denom - tripleARTbefPregNum - tripleARTdurPregNum) > 0:
                if 'optionA' in num_treatments_BF:
                    treatPercent['optionA_BF'] = postnatalProph['optionA'][t-minYear] / (denom - tripleARTbefPregNum - tripleARTdurPregNum)
                if 'optionB' in num_treatments_BF:
                    treatPercent['optionB_BF'] = postnatalProph['optionB'][t-minYear] / (denom - tripleARTbefPregNum - tripleARTdurPregNum)
            else:
                if 'optionA' in num_treatments_BF:
                    treatPercent['optionA_BF'] = 0
                if 'optionB' in num_treatments_BF:
                    treatPercent['optionB_BF'] = 0
            for p in num_treatments:
                treatPercent[p] = prenatalProph[p][t-minYear] / denom
        for p in percent_treatments:
            treatPercent[p] = prenatalProph[p][t-minYear] / 100
        for p in percent_treatments_BF:
            treatPercent[p] = postnatalProph[p][t-minYear] / 100

        tempSum = 0
        for p in treatPercent:
            if p != 'noProph':
                treatPercent[p] = min(1, treatPercent[p])
                tempSum += treatPercent[p]
        treatPercent['noProph'] = max(0, 1 - tempSum)

        BFTR = calcBFtransmission(1, 3, t)

        # Calculate infections by different time periods
        newInfFromBFLT6 = max(0, (tempBirths - HIVbirths) * BFTR)
        if stage == 'stage_2' and parent != 'IND':
            newChildHIV['male'][0] += min(newInfFromBFLT6 * (1 - SRB[t]) * childIncAdj[t][0]['male'], capHIV * (1 - SRB[t]))
            newChildHIV['female'][0] += min(newInfFromBFLT6 * SRB[t] * childIncAdj[t][0]['female'], capHIV * SRB[t])
            newInfFromBFLT6 = min(newInfFromBFLT6 * (1 - SRB[t]) * childIncAdj[t][0]['male'], (tempBirths - HIVbirths) * (1 - SRB[t])) + min(newInfFromBFLT6 * SRB[t] * childIncAdj[t][0]['female'], (tempBirths - HIVbirths) * SRB[t])
            capHIV = max(capHIV - newInfFromBFLT6, 0)
        else:
            newChildHIV['male'][0] += newInfFromBFLT6 * (1 - SRB[t])
            newChildHIV['female'][0] += newInfFromBFLT6 * SRB[t]
        cumulNewInfFromBF = newInfFromBFLT6

        BFTR = calcBFtransmission(4, 6, t)
        newInfFromBF6to12 = max(0, (tempBirths - HIVbirths - newInfFromBFLT6) * BFTR)
        if stage == 'stage_2' and parent != 'IND':
            newChildHIV['male'][0] += min(newInfFromBF6to12 * (1 - SRB[t]) * childIncAdj[t][0]['male'], capHIV * (1-SRB[t]))
            newChildHIV['female'][0] += min(newInfFromBF6to12 * SRB[t] * childIncAdj[t][0]['female'], capHIV * SRB[t])
            newInfFromBF6to12 =  min(newInfFromBF6to12 * (1 - SRB[t]) * childIncAdj[t][0]['male'], capHIV * (1-SRB[t])) + min(newInfFromBF6to12 * SRB[t] * childIncAdj[t][0]['female'], capHIV * SRB[t])
            capHIV = max(capHIV - newInfFromBF6to12, 0)
        else:
            newChildHIV['male'][0] += newInfFromBF6to12 * (1 - SRB[t])
            newChildHIV['female'][0] += newInfFromBF6to12 * SRB[t]
        cumulNewInfFromBF += newInfFromBF6to12

        if (cumulNewInfFromBF + HIVbirths) <= 0:
            propNewInfFromBF = 0
        else:
            propNewInfFromBF = cumulNewInfFromBF / (cumulNewInfFromBF + HIVbirths)

        if HIVbirths > 0:
            PTR = PTR * (1 + cumulNewInfFromBF / HIVbirths)

        AIDSbirths = {}
        AIDSbirths['both'] = HIVbirths
        AIDSbirths['female'] = AIDSbirths['both'] * SRB[t]
        AIDSbirths['male'] = AIDSbirths['both'] - AIDSbirths['female']

        for sex in sexes:
            sr = survivalRates[0][t][sex]
            for c in u5CD4States:
                population[0][t][sex][c]["perinatal"][t-minYear] = AIDSbirths[sex] * sr * CD4PercAtInfection[c]

        for c in u5CD4States:
            population[0][t]['female'][c]['BF0'][t-minYear] = newInfFromBFLT6 * SRB[t] * CD4PercAtInfection[c]
            population[0][t]['male'][c]['BF0'][t-minYear] = newInfFromBFLT6 * (1 - SRB[t]) * CD4PercAtInfection[c]
            population[0][t]['female'][c]['BF7'][t-minYear] = newInfFromBF6to12 * SRB[t] * CD4PercAtInfection[c]
            population[0][t]['male'][c]['BF7'][t-minYear] = newInfFromBF6to12 * (1 - SRB[t]) * CD4PercAtInfection[c]

        for sex in sexes:
            population[0][t][sex]['neg'] -= sum([sum(population[0][t][sex][c]["perinatal"]) for c in u5CD4States])  + sum([sum(population[0][t][sex][c]["BF0"]) for c in u5CD4States]) + sum([sum(population[0][t][sex][c]["BF7"]) for c in u5CD4States])
            if population[0][t][sex]['neg'] < 0:
                population[0][t][sex]['neg'] = 0

        if getBirths(t, 'total') <= 0:
            percentExposed = 0
        else:
            percentExposed = (tempBirths - HIVbirths - cumulNewInfFromBF) / getBirths(t, 'total')
        BFTR = calcBFtransmission(7, 12, t)

        a = 1
        for sex in sexes:
            if stage == 'stage_2' and parent != 'IND':
                newInfFromBFGT12 = max(0, min(population[a][t][sex]['neg'] * percentExposed * BFTR * childIncAdj[t][1][sex], population[a][t][sex]['neg'] * percentExposed))
            else:
                newInfFromBFGT12 = max(0, population[a][t][sex]['neg'] * percentExposed * BFTR)
            newChildHIV[sex][0] += newInfFromBFGT12 
            for c in u5CD4States:
                population[a][t][sex][c]["BF12"][t-minYear] = newInfFromBFGT12 * CD4PercAtInfection[c]
            population[a][t][sex]['neg'] -= newInfFromBFGT12
            cumulNewInfFromBF += newInfFromBFGT12

        percentExposed = percentExposed * (1 - BFTR)
        BFTR = calcBFtransmission(13, 18, t)
        a = 2

        for sex in sexes:
            if stage == 'stage_2' and parent != 'IND':
                newInfFromBFGT12 = max(0, min(population[a][t][sex]['neg'] * percentExposed * BFTR * childIncAdj[t][1][sex], population[a][t][sex]['neg'] * percentExposed))
            else:
                newInfFromBFGT12 = max(0, population[a][t][sex]['neg'] * percentExposed * BFTR)
            newChildHIV[sex][0] += newInfFromBFGT12 
            for c in u5CD4States:
                population[a][t][sex][c]["BF12"][t-minYear] += newInfFromBFGT12 * CD4PercAtInfection[c]

            population[a][t][sex]['neg'] -= newInfFromBFGT12
            cumulNewInfFromBF += newInfFromBFGT12

        #Calculating need for child ART
        unmetNeed = 0
        for age in range(0,5):
            #Children needing ART above age threshold based on CD4 eligibility
            if ((age + 1) * 12) > childEligibilityAge[t-minYear]:
                CD4Elig = findU5EligibilityGroups(age, t, childEligibilityPercent, u5CD4States)
                unmetNeed += sum([sum([sum([sum(population[age][t][sex][c][d]) for d in noARTChildHIVStates]) for c in CD4Elig]) for sex in sexes])
            #All children under a certain age threshold need ART
            else:
                unmetNeed += sum([sum([sum([sum(population[age][t][sex][c][d]) for d in noARTChildHIVStates]) for c in u5CD4States]) for sex in sexes])


        for age in range(5, 15):
            #Children needing ART above age threshold based on CD4 eligibility
            if ((age + 1) * 12) > childEligibilityAge[t-minYear]:
                CD4Elig = findU15EligibilityGroups(age, t, childEligibilityCount, u15CD4States)
                unmetNeed += sum([sum([sum([sum(population[age][t][sex][c][d]) for d in noARTChildHIVStates]) for c in CD4Elig]) for sex in sexes])
            #All children under a certain age threshold need ART
            else:
                unmetNeed += sum([sum([sum([sum(population[age][t][sex][c][d]) for d in noARTChildHIVStates]) for c in u15CD4States]) for sex in sexes])

                        # If the age threshold is not a multiple of 12, calculate the proportion of chilren in each age group that are eligible
            # (This section returns 0 if the age threshold is a multiple of 12) 
    #         if ((age * 12) <= childEligibilityAge[t-minYear]) and ((a + 1) * 12 > childEligibilityAge[t-minYear]):
    #             for i in xrange(minYear, t+1):
    #                 population[age][t]['both']['needTx']['all'][i-minYear] += (childEligibilityAge[t-minYear] - age * 12) / 12 * (population[age][t]['both']['asym']['all'][i-minYear]
    #                     + population[age][t]['both']['asymBFLT6Mo']['all'][i-minYear] + population[age][t]['both']['asymBF6to12Mo']['all'][i-minYear]
    #                     + population[age][t]['both']['asymBFGT12Mo']['all'][i-minYear])


        # Calculate child ART coverage
        onFLART = sum([sum([sum([sum([sum(population[age][t][sex][c][d]) for d in adultARTdurations]) for c in u5CD4States]) for sex in sexes]) for age in range(0,5)])
        onFLART += sum([sum([sum([sum([sum(population[age][t][sex][c][d]) for d in adultARTdurations]) for c in u15CD4States]) for sex in sexes]) for age in range(5,15)])
        needForFLART = unmetNeed + onFLART             

        allInterventionCoverage[t]['child']['female']['ART']['eligible'] += needForFLART

        #Calculate number added to FLART from input which may be % or #
        if childARTcoverageType['ART'][t-1-minYear] == 'percent':
            ARTlastYear = needForFLART * childARTcoverage['ART'][t-1-minYear] / 100
        else:
            ARTlastYear = childARTcoverage['ART'][t-1-minYear]
        if childARTcoverageType['ART'][t-minYear] == 'percent':
            ARTthisYear = needForFLART * childARTcoverage['ART'][t-minYear] / 100
        else:
            ARTthisYear = childARTcoverage['ART'][t-minYear]
        allInterventionCoverage[t]['child']['female']['ART']['coverage'] = ARTthisYear
        newFLART = ((ARTthisYear + ARTlastYear) / 2 ) - onFLART
        if newFLART < 0:
            newFLART = 0

        onARTDeaths = 0
        for i in range(minYear, t + 1):

            v1 = sum([sum([sum([sum([population[age][t][sex][c][d][i - minYear] * childHIVMort[age][c][sex][d] for d in adultARTdurations]) for c in u5CD4States]) for sex in sexes]) for age in range(0,5)])
            v2 = sum([sum([sum([sum([population[age][t][sex][c][d][i - minYear] * childHIVMort[age][c][sex][d] for d in adultARTdurations]) for c in u15CD4States]) for sex in sexes]) for age in range(5,15)])
            onARTDeaths += (v1 + v2) / 2

        newFLART += onARTDeaths
        if needForFLART < onFLART + newFLART:
            needForFLART = onFLART + newFLART

        #Distribute according to IeDEA data
        temp = 0
        for age in range (0, 15):
            age1 = monthConcordance(age)
            for sex in sexes:
                if age < 5:
                    CD4Elig = findU5EligibilityGroups(age, t, childEligibilityPercent, u5CD4States)
                    for c in u5CD4States:
                        if ((age + 1) * 12) <= childEligibilityAge[t-minYear] or c in CD4Elig: 
                            temp += childARTDistribution[min(age + 1, 14)][t]  * sum([sum(population[age][t][sex][c][d]) for d in noARTChildHIVStates])
                else:
                    CD4Elig = findU15EligibilityGroups(age, t, childEligibilityCount, u15CD4States)
                    for c in u15CD4States:
                        if ((age + 1) * 12) <= childEligibilityAge[t-minYear] or c in CD4Elig: 
                              temp += childARTDistribution[min(age + 1, 14)][t] * sum([sum(population[age][t][sex][c][d]) for d in noARTChildHIVStates])
        adj = 1
        if temp > 0:
            adj = newFLART / temp

        for age in range (0, 15):
            age1 = monthConcordance(age)
            for sex in sexes:
                for d in noARTChildHIVStates:
                    if age < 5:
                        CD4Elig = findU5EligibilityGroups(age, t, childEligibilityPercent, u5CD4States)
                        for c in u5CD4States:
                            if ((age + 1) * 12) <= childEligibilityAge[t-minYear] or c in CD4Elig: 
                                newChildART[age][sex][c][d] = min(1, adj * childARTDistribution[min(age + 1, 14)][t]) * sum(population[age][t][sex][c][d])
    #                                 for i in range(minYear, t + 1):
    #                                     population[age][t][sex][c][d][i - minYear] -= newChildART[age][sex][c][d] * (population[age][t][sex][c][d][i-minYear]/sum(population[age][t][sex][c][d]))
    #                                     if population[age][t][sex][c][d][i - minYear] < 0:
    #                                         population[age][t][sex][c][d][i - minYear] = 0
                    else:
                        CD4Elig = findU15EligibilityGroups(age, t, childEligibilityCount, u15CD4States)
                        for c in u15CD4States:
                            if ((age + 1) * 12) <= childEligibilityAge[t-minYear] or c in CD4Elig: 
                                newChildART[age][sex][c][d] = min(1, adj * childARTDistribution[min(age + 1, 14)][t]) * sum(population[age][t][sex][c][d])
    #                                 for i in range(minYear, t + 1):
    #                                     population[age][t][sex][c][d][i - minYear] -= newChildART[age][sex][c][d] * (population[age][t][sex][c][d][i-minYear]/sum(population[age][t][sex][c][d]))
    #                                     if population[age][t][sex][c][d][i - minYear] < 0:
    #                                         population[age][t][sex][c][d][i - minYear] = 0
        # Get CTX coverage

        posU5pop = sum([sum([sum([sum([sum(population[age][t][sex][c][d]) for d in noARTChildHIVStates]) for c in u5CD4States]) for sex in sexes]) for age in range(0,5)])
        eligible5to15 = 0
        for age in xrange(5, 15):
            CD4Elig = findU15EligibilityGroups(age, t, childEligibilityCount, u15CD4States)
            for c in u15CD4States:
                if ((age + 1) * 12) <= childEligibilityAge[t-minYear] or c in CD4Elig: 
                      eligible5to15 += sum([sum([sum(population[age][t][sex][c][d]) for d in noARTChildHIVStates]) for sex in sexes])

        needCTX = posU5pop + eligible5to15
        allInterventionCoverage[t]['child']['female']['CTX']['eligible'] = needCTX
        allInterventionCoverage[t]['child']['female']['CTX']['coverage'] = min(childARTcoverage['CTX'][t-minYear], needCTX)

        if needCTX > 0:
            CTXcoverage = min(1, childARTcoverage['CTX'][t-minYear] / needCTX)
        else:
            CTXcoverage = 0
        fractionProgressing = 0

        age0fractions = []


        # Calculate child HIV/AIDS deaths and progression
        for sex in sexes:
            for age in range(0, childMaxAge):
                age5 = age - age % 5
                if age < 5:
                    CD4States = u5CD4States
                else:
                    CD4States = u15CD4States
                #Those not on ART       
                for d in noARTChildHIVStates:
                    for i in range(minYear, t + 1):
                        for c in CD4States:
                            #v2 are progressors in from higher CD4 category
                            if c == "GT30" or c== "GT1000CD4":
                                entrantsChild = 0
                            else:
                                entrantsChild = v2
                            #v1 is AIDS deaths
                            v1 = population[age][t][sex][c][d][i - minYear] * childHIVMort[age][c][sex][d]
                            #Adjust mortality for effects of cotrimoxazole
                            v1 = v1 * (1 - (childCTXeffect['noART'] * CTXcoverage))
                            #v2 is progression out to lower CD4 categories
                            if c!= "LT5" and c!= "LT200CD4":
                                v2 = population[age][t][sex][c][d][i - minYear] *  childProgParam[age][sex][c]
                            else:
                                v2 = 0
                            exitsChild = v1 + v2 + (newChildART[age][sex][c][d] * min(1, population[age][t][sex][c][d][i-minYear]/max(.0000000001, sum(population[age][t][sex][c][d]))))
                            AIDSdeaths[age][t][sex][i-minYear] += max(0, v1)
                            AIDSdeathsCD4[age5][t][sex][c][d] += max(0, v1)
                            population[age][t][sex][c][d][i - minYear] = population[age][t][sex][c][d][i - minYear] + entrantsChild - exitsChild
                            if population[age][t][sex][c][d][i - minYear] < 0:
                                population[age][t][sex][c][d][i - minYear] = 0
                #Those on ART            
                for i in range(minYear, t + 1):
                    for c in CD4States:
                        #Entrants to LT6M category are new ART patients
                        entrants6M = sum([newChildART[age][sex][c][d] for d in noARTChildHIVStates]) * min(1, (sum([population[age][t][sex][c][d][i-minYear] for d in noARTChildHIVStates])/max(.00000000001, sum([sum(population[age][t][sex][c][d]) for d in noARTChildHIVStates]))))

                        #Entrants are subject to mortality
                        #v2 is deaths in those entering LT6Mo group
            
                        v2 = entrants6M * ((childHIVMort[age][c][sex]['LT6Mo'] + childHIVMort[age][c][sex]["6to12Mo"] ) / 2) 
                        #Add cotrimoxazole effect
                        v2 = v2 * (1 - CTXcoverage * childCTXeffect['onART'])
                        entrants6M -= v2

                        #Estimate mortality of those on ART LT6Mo
                        #v1 is deaths in those entering GT12Mo group
                        v1 = population[age][t][sex][c]['LT6Mo'][i-minYear] * ((childHIVMort[age][c][sex]['LT6Mo'] + childHIVMort[age][c][sex]["6to12Mo"] ) / 2)  
                        v1 = v1 * (1 - CTXcoverage * childCTXeffect['onART'])
                        #Entrants to GT12Mo are those surviving from LT6Mo last year
                        entrants12M = population[age][t][sex][c]["LT6Mo"][i-minYear] - v1
                        AIDSdeaths[age][t][sex][i - minYear] += max(0, (v1 + v2))
                        AIDSdeathsCD4[age5][t][sex][c]['ART'] += max(0, (v1 + v2))
                        #Adding new patients to ART LT6M
                        population[age][t][sex][c]['LT6Mo'][i - minYear] = entrants6M

                        #Exits are those dying on ART
                        exits12M = population[age][t][sex][c]['GT12Mo'][i-minYear] * childHIVMort[age][c][sex]["GT12Mo"]
                        #Add effect of cotrimoxazole
                        exits12M = exits12M * (1 - CTXcoverage * childCTXeffect['onART'])
                        AIDSdeaths[age][t][sex][i-minYear] += max(0, exits12M)
                        AIDSdeathsCD4[age5][t][sex][c]['ART'] += max(0, exits12M)
                        population[age][t][sex][c]["GT12Mo"][i-minYear] = population[age][t][sex][c]["GT12Mo"][i-minYear] + entrants12M - exits12M


        updateAllStateTotal(t)

        # Update cohort data out object
        for a in xrange(0, maxAge+1):
            for s in sex_nums.keys():
                tmp_row = [individual_id, t, a, sex_nums[s]]
                tmp_deaths = []
                for i in xrange(minYear, maxYear+1):
                    tmp_deaths.append(AIDSdeaths[a][t][s][i-minYear])
                tmp_row.extend(tmp_deaths)
                cohortData.append(tmp_row)



        for sex in sexes:
            for age in xrange(0, 15, 5):
                if age == 0:
                    tmpBirthPrev = hivBirthPrevOut[sex]
                else:
                    tmpBirthPrev = 0
                popData = []
                tmpNewHIV = newChildHIV[sex][age]
                tmpHIVbirths = 0
                tmpTotalBirths = 0
                tmpDeaths = 0
                tmpSusceptPop = susceptiblePop[sex][age]
                tmpNonAIDSdeaths = 0
                for p in ARTCD4states:
                    tmpPonART[p] = 0
                for c in out_cats:
                    tmpPop[c] = 0
                for a5 in xrange(age, age + 5):
                    tmpPopSA = 0
                    tmpDeaths += sum(AIDSdeaths[a5][t][sex])
                    tmpNonAIDSdeaths += deaths[a5][t][sex]
                    tmpPop['neg'] += population[a5][t][sex]['neg']
                    if age < 5:
                        CD4States = u5CD4States
                        tmpPop['LT200CD4'] += sum([sum(population[a5][t][sex]["LT5"][d]) for d in noARTChildHIVStates]) + sum([sum(population[a5][t][sex]["5to10"][d]) for d in noARTChildHIVStates]) + (1/9 * sum([sum(population[a5][t][sex]["11to15"][d]) for d in noARTChildHIVStates]))
                        tmpPop['200to350CD4'] += (8/9 * sum([sum(population[a5][t][sex]["11to15"][d]) for d in noARTChildHIVStates])) + (3/13 * sum([sum(population[a5][t][sex]["16to20"][d]) for d in noARTChildHIVStates]))
                        tmpPop['GT350CD4'] +=  (10/13 * sum([sum(population[a5][t][sex]["16to20"][d]) for d in noARTChildHIVStates])) + sum([sum([sum(population[a5][t][sex][c][d]) for d in noARTChildHIVStates]) for c in u5CD4States[:3]])                             
                    else:
                        CD4States = u15CD4States
                        tmpPop['LT200CD4'] += sum([sum(population[a5][t][sex]["LT200CD4"][d]) for d in noARTChildHIVStates])
                        tmpPop['200to350CD4'] += sum([sum(population[a5][t][sex]["200to349CD4"][d]) for d in noARTChildHIVStates])
                        tmpPop['GT350CD4'] += sum([sum([sum(population[a5][t][sex][c][d]) for d in noARTChildHIVStates]) for c in u15CD4States[:4]])
                    for c in CD4States:
                            for d in noARTChildHIVStates:
                                tmpPopSA += sum(population[a5][t][sex][c][d])
                            for d in adultARTdurations:
                                tmpPop['ART'] += sum(population[a5][t][sex][c][d])

                for c in out_cats:
                    popData.append(tmpPop[c])
                out_data = [individual_id, t, sex, age, tmpDeaths, tmpNewHIV, tmpHIVbirths, tmpSusceptPop, tmpNonAIDSdeaths, tmpTotalBirths, tmpBirthPrev]
                out_data.extend(popData)
                csvData.append(out_data)
            for age in xrange(15, 80, 5):
                tmpBirthPrev = 0
                popData = []
                tmpNewHIV = newHIVbyAge5[sex][age]
                tmpHIVbirths = birthsToPosMothers[sex][age]
                tmpTotalBirths = 0
                if sex == 'female' and age < 50:
                    tmpTotalBirths = birthsToAllMothers[sex][age]
                tmpDeaths = 0
                tmpSusceptPop = susceptiblePop[sex][age]
                tmpNonAIDSdeaths = 0
                for a5 in xrange(age, age + 5):
                    tmpDeaths += sum(AIDSdeaths[a5][t][sex])
                    tmpNonAIDSdeaths += deaths[a5][t][sex]
                for c in out_cats:
                    tmpPop[c] = 0
                for c in noARTCD4statesNeg:
                    if c != 'neg':
                        lower_limit = CD4lowerLimits[c]
                        if lower_limit < 200:
                            out_cat = 'LT200CD4'
                        elif lower_limit >= 200 and lower_limit < 350:
                            out_cat = '200to350CD4'
                        else:
                            out_cat = 'GT350CD4'
                    else:
                        out_cat = 'neg'
                    for a5 in xrange(age, age + 5):
                        if c == 'neg':
                            tmpPop[out_cat] += population[a5][t][sex][c]
                        else:
                            tmpPop[out_cat] += sum(population[a5][t][sex][c])
                for c in ARTCD4states:
                    for d in adultARTdurations:
                        for a5 in xrange(age, age + 5):
                            tmpPop['ART'] += sum(population[a5][t][sex][c][d])
                for c in out_cats:
                    popData.append(tmpPop[c])
                out_data = [individual_id, t, sex, age, tmpDeaths, tmpNewHIV, tmpHIVbirths, tmpSusceptPop, tmpNonAIDSdeaths, tmpTotalBirths, tmpBirthPrev]
                out_data.extend(popData)
                csvData.append(out_data)
            age = 80
            popData = []
            tmpBirthPrev = 0
            tmpNewHIV = 0
            tmpHIVbirths = 0
            tmpTotalBirths = 0
            tmpDeaths = 0
            tmpDeaths += sum(AIDSdeaths[age][t][sex])
            tmpSusceptPop = susceptiblePop[sex][age]
            tmpNonAIDSdeaths = deaths[age][t][sex]
            for c in out_cats:
                tmpPop[c] = 0
            for c in noARTCD4statesNeg:
                if c != 'neg':
                    lower_limit = CD4lowerLimits[c]
                    if lower_limit < 200:
                        out_cat = 'LT200CD4'
                    elif lower_limit >= 200 and lower_limit < 350:
                        out_cat = '200to350CD4'
                    else:
                        out_cat = 'GT350CD4'
                else:
                    out_cat = 'neg'
                if c == 'neg':
                    tmpPop[out_cat] += population[age][t][sex][c]
                else:
                    tmpPop[out_cat] += sum(population[age][t][sex][c])
            for c in ARTCD4states:
                for d in adultARTdurations:
                    tmpPop['ART'] += sum(population[age][t][sex][c][d])
            for c in out_cats:
                popData.append(tmpPop[c])
            out_data = [individual_id, t, sex, age, tmpDeaths, tmpNewHIV, tmpHIVbirths, tmpSusceptPop, tmpNonAIDSdeaths, tmpTotalBirths, tmpBirthPrev]
            out_data.extend(popData)
            csvData.append(out_data)

        tmpPop0 = 0
        sex = 'male'

        # Prep coverage data for output
        for age in xrange(0, 5):
            tmpPop0 += population[age][t][sex]["neg"]
            for c in u5CD4States:       
                tmpPop0 += sum([sum(population[age][t][sex][c][d]) for d in noARTChildHIVStates])
        for sex in sexes:
            allInterventionCoverage[t]['adult'][sex]['ART']['eligible'] = 0
        for age in xrange(0, maxAge + 1):
            for sex in sexes:
                if age < 5:
                    allInterventionCoverage[t]['child'][sex]['ART']['coverage'] += sum([sum([sum(population[age][t][sex][c][d]) for d in adultARTdurations]) for c in u5CD4States])
                elif age < 15 and age >=5:
                    allInterventionCoverage[t]['child'][sex]['ART']['coverage'] += sum([sum([sum(population[age][t][sex][c][d]) for d in adultARTdurations]) for c in u15CD4States])
                elif age >= 15:
                    for c in ARTCD4states:
                        for d in adultARTdurations:
                            # allInterventionCoverage[t]['adult'][sex]['ART']['coverage'] += population[age][t][sex][c][d]
                            allInterventionCoverage[t]['adult'][sex]['ART']['eligible'] += sum(population[age][t][sex][c][d])
                    for c in noARTCD4states:
                        allInterventionCoverage[t]['adult'][sex]['ART']['eligible'] += sum(population[age][t][sex][c])
                        # if CD4lowerLimits[c] < adultARTeligibility[t-minYear]:
        for sex in sexes:
            allInterventionCoverage[t]['adult'][sex]['ART']['coverage'] = min(allInterventionCoverage[t]['adult'][sex]['ART']['eligible'], currentYearART[sex])
            # allInterventionCoverage[t]['adult'][sex]['ART']['eligible'] = need15plus[t][sex]
        for a in ['adult', 'child']:
            for sex in sexes:
                for c in allInterventions:
                    coverageData.append([individual_id, t, a, sex, c, allInterventionCoverage[t][a][sex][c]['coverage'], allInterventionCoverage[t][a][sex][c]['eligible']])

    if detailed_output:
        ## Output coverage data for forecasting
        out_list = []
        # Extract values and most granular level
        for a in xrange(0, maxAge+1):
                for t in xrange(minYear, maxYear+1):
                        for s in sexes:
                                val = population[a][t][s]['neg']
                                out_list.append([a,t,s,'neg','',val])
                                if a < 5:
                                    for c in u5CD4States:
                                        for d in adultARTdurations:
                                            val = sum(population[a][t][s][c][d])
                                            out_list.append([a,t,s,str('ART' + c),d,val])
                                        for d in noARTChildHIVStates:
                                            val = sum(population[a][t][s][c][d])
                                            out_list.append([a,t,s,c,'',val])
                                if a >= 5 and a < 15:
                                     for c in u15CD4States:
                                        for d in adultARTdurations:
                                            val = sum(population[a][t][s][c][d])
                                            out_list.append([a,t,s,str('ART' + c),d,val])
                                        for d in noARTChildHIVStates:
                                            val = sum(population[a][t][s][c][d])
                                            out_list.append([a,t,s,c,'',val])
                                if a >= 15:
                                    for c in allCD4states:
                                            if c in noARTCD4states:
                                                    d=''
                                                    val = sum(population[a][t][s][c])
                                                    out_list.append([a,t,s,c,d,val])
                                            elif c in ARTCD4states:
                                                    for d in adultARTdurations:
                                                            val = sum(population[a][t][s][c][d])
                                                            out_list.append([a,t,s,c,d,val])
        cov_data = pd.DataFrame(out_list, columns=['age', 'year', 'sex', 'CD4', 'duration', 'pop'])  # convert to dataframe
        cov_data = cov_data.groupby(by=['age', 'year', 'sex', 'CD4']).sum().reset_index()  # add up durations within CD4 count
        cov_data['treatment'] = cov_data['CD4'].apply(lambda x: 1 if 'ART' in x else 0)  # extract treatment from CD4 variable
        cov_data['CD4'] = cov_data['CD4'].apply(lambda x: x.split('ART')[1] if 'ART' in x else x)  # rename CD4 without treatment info
        cov_data = cov_data.set_index(['age','sex','year','CD4','treatment']).unstack('treatment')  # reshape treatment wide
        cov_data.columns = ['pop_0','pop_1']  # reset columns
        cov_data['run_num'] = individual_id
        cov_data['iso3'] = ISO
        cov_data.reset_index()
        appended_cov_data.append(cov_data)  # store dataframes in list


        ## Output deaths for forecasting
        deaths_out_list = []
        for a in xrange(0, maxAge+1):
                for t in xrange(minYear, maxYear+1):
                        for s in sexes:
                                val = sum(AIDSdeaths[a][t][s])
                                deaths_out_list.append([a,t,s,val])
        deaths_data = pd.DataFrame(deaths_out_list, columns=['age', 'year', 'sex', 'deaths'])
        deaths_data['run_num'] = individual_id
        deaths_data['iso3'] = ISO
        appended_deaths_data.append(deaths_data)


    #
    if art_deaths_output:
        ARTDeaths = []
        for a in range(0, 81, 5):
            for t in range(minYear, maxYear + 1):
                for sex in sexes:
                    if a >= 15:
                        for c in noARTCD4states:
                            ARTDeaths.append([t, a, sex_nums[sex], c, AIDSdeathsCD4[a][t][sex][c]])
                        for c in ARTCD4states:
                            ARTDeaths.append([t, a, sex_nums[sex], c, sum([AIDSdeathsCD4[a][t][sex][c][d] for d in adultARTdurations])])
                    elif a < 15 and a >= 5:
                        for c in u15CD4States:
                            ARTDeaths.append([t, a, sex_nums[sex], c, sum([AIDSdeathsCD4[a][t][sex][c][d] for d in noARTChildHIVStates])])
                            ARTDeaths.append([t, a, sex_nums[sex], 'ART' + c, AIDSdeathsCD4[a][t][sex][c]['ART']])      
                    else:
                        for c in u5CD4States:
                            ARTDeaths.append([t, a, sex_nums[sex], c, sum([AIDSdeathsCD4[a][t][sex][c][d] for d in noARTChildHIVStates])])
                            ARTDeaths.append([t, a, sex_nums[sex], 'ART' + c, AIDSdeathsCD4[a][t][sex][c]['ART']])      
        art_deaths_data = pd.DataFrame(ARTDeaths, columns=['year', 'age', 'sex_id', 'CD4', 'Deaths'])
        art_deaths_data['run_num'] = individual_id
        art_deaths_data['ihme_loc_id'] = ISO
        appended_art_deaths_data.append(art_deaths_data)

    ## End of run loop








# ## Write ouput data

# In[7]:

# Combine cov_data list
if detailed_output:
    appended_cov_data = pd.concat(appended_cov_data)
    appended_deaths_data = pd.concat(appended_deaths_data)
if art_deaths_output:
    appended_art_deaths_data = pd.concat(appended_art_deaths_data)

results_dir = os.path.split(directory)[-1]
# result_path = '/clustertmp/WORK/04_epi/01_database/02_data/hiv/01_lit/Spectrum_output/' + results_dir +'/'
result_path = '/ihme/hiv/spectrum_draws/' + folder
if not os.path.isdir(result_path):
    try:
        os.mkdir(result_path)
    except:
        pass
if detailed_output:
    # Write granular coverage data to new directory
    cov_dir = result_path + '/detailed_art_coverage/' + ISO + '/'
    if not os.path.isdir(cov_dir):
            os.makedirs(cov_dir)
    appended_cov_data.to_csv(cov_dir + str(run_num) + '_cov_data.csv')

    # Write deaths data to new directory
    deaths_dir = result_path + '/detailed_deaths/' + ISO + '/'
    if not os.path.isdir(deaths_dir):
            os.makedirs(deaths_dir)
    appended_deaths_data.to_csv(deaths_dir + str(run_num) + '_deaths_data.csv')


result_path = result_path + '/draws/'
print result_path
if not os.path.isdir(result_path):
    try:
        os.mkdir(result_path)
    except:
        pass
stage_path = result_path + stage + '/'
if not os.path.isdir(stage_path):
    try:
        os.mkdir(stage_path)
    except:
        pass
writePath = stage_path + ISO + '/'
if not os.path.isdir(writePath):
    try:
        os.mkdir(writePath)
    except:
        pass

file_name = ISO + '_ART_deaths_' + str(run_num) + '.csv'
testFile = open(writePath + file_name, 'w')
wr = csv.writer(testFile)
for row in csvData:
    wr.writerow(row)
testFile.close()

cov_file_name = ISO + '_coverage_' + str(run_num) + '.csv'
coverageFile = open(writePath + cov_file_name, 'w')
wr = csv.writer(coverageFile)
for row in coverageData:
    wr.writerow(row)
coverageFile.close()

if cohort_output:
    cohort_file_name = ISO + '_cohort_' + str(run_num) + '.csv'

    testFile = open(writePath + cohort_file_name, 'w')
    wr = csv.writer(testFile)
    for row in cohortData:
        wr.writerow(row)
    testFile.close()


if art_deaths_output:
    art_deaths_file_name = ISO + '_on_off_ART_deaths_' + str(run_num) + '.csv'
    appended_art_deaths_data.to_csv(writePath + art_deaths_file_name)


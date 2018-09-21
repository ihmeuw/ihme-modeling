CODEm Hybridizer
=====

This repo contains code to hybridize a global and data-rich CODEm model

## Components
  1. run_hybridizer.sh: this is the script that should be called by CoDViz and takes the user, 
        the global model number, and the data-rich model number (in that order) as arguments.  
        Example usage:
            sh ./run_hybridizer.sh [USER] [GLOBAL MODEL NUMBER] [DATA-RICH MODEL NUMBER]
            
  2. submit_hybrid.py: this script is called by run_hybridizer and submits the hybridizer
        jobs to the cluster on behalf of the user.
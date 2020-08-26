
// File:		format_ICD10_codes.do
// Project:	GBD - Cancer Registry
** Description:	Format code ranges and ensure that most ICD10 codes are in the proper format for subtotal disaggregation

** *****************************************************************************
** Configuration
** *****************************************************************************
// Accept or Set Arguments
	args letter causeVar

	if "`letter'" == "" {
		local letter = "C"
		local causeVar = "var1"
	}

	gen updateVar = `causeVar' if coding_system == "ICD10"

** *****************************************************************************
** Format Codes 
**	
** *****************************************************************************
// Part1: Add commas and letters between distinct codes that are adjacent
	quietly foreach 1 of numlist 0/9 {
		replace updateVar = subinstr(updateVar, "`1'`letter'", "`1',`letter'", .)
		foreach 2 of numlist 0/9 {
			replace updateVar = subinstr(updateVar, "`1',`2'", "`1',`letter'`2'", .)
		}							
	}

// Part2: Add leading zeroes for single digit codes
	quietly foreach number of numlist 0/9 {
		replace updateVar = "`letter'0`number'" if updateVar == "`letter'`number'"				// only entry
		replace updateVar = subinstr(updateVar, "`letter'`number',", "`letter'0`number',", .)		// first or middle entry
		replace updateVar = subinstr(updateVar, "`letter'`number'.", "`letter'0`number'.", .)		// has decimal
		replace updateVar = subinstr(updateVar, "`letter'`number'-", "`letter'0`number'-", .)		// part of range
		replace updateVar = "`letter'0`number'" if substr(updateVar, -2, .) == "`letter'`number'"		// last entry
	}
	
// Part3: Add decimals to 3 digit codes, add letters between distinct codes that are adjacent, correct certain code ranges
	quietly foreach 1 of numlist 0/9 {
		foreach 2 of numlist 0/9 {
			foreach 3 of numlist 0/9 {
				// Add decimals to 3 digit codes
					replace updateVar = subinstr(updateVar,"`letter'`1'`2'`3'","`letter'`1'`2'.`3'", .)
					replace updateVar = subinstr(updateVar,"-`1'`2'`3'","-`letter'`1'`2'.`3'", .)
			}
		}
	}	
						
// // Part4: Add letters to each end of a code range. LIMITATIONS: does not format a small number of cases that need human interpretation (for example C01.11-12)
	quietly foreach 1 of numlist 0/9 {
		foreach 2 of numlist 0/9 {
			foreach 3 of numlist 0/9 {
				foreach 4 of numlist 0/9 {
					// Correct range only if the second element is greater or equal to the first
						if `3' > `1' | (`3' == `1' & `4' > `2') replace updateVar=subinstr(updateVar,"`letter'`1'`2'-`3'`4'","`letter'`1'`2'-`letter'`3'`4'", .) 
					// Correct non-decimal range
						if `3' > `2' replace updateVar=subinstr(updateVar,"`letter'`1'`2'-`3'","`letter'`1'`2'-`letter'`1'`3'",.) if substr(updateVar, -5, .) == "`letter'`1'`2'-`3'"  // only for last entry
						replace updateVar=subinstr(updateVar,"`letter'`1'`2'-`3',","`letter'`1'`2'-`letter'`1'`3',",.)  // first or middle entry
					// Correct decimal range
						if `4' >= `3' {
							// Correct decimal range if the decimal is not explicit
								replace updateVar=subinstr(updateVar, "`letter'`1'`2'.`3'-`4'", "`letter'`1'`2'.`3'-`letter'`1'`2'.`4'",.) if substr(updateVar, -7, .) == "`letter'`1'`2'.`3'-`4'"   // only or last entry
								replace updateVar=subinstr(updateVar, "`letter'`1'`2'.`3'-`4',", "`letter'`1'`2'.`3'-`letter'`1'`2'.`4',",.)   // first or middle entry
							// Correct decimal range if the decimal is explicit (part 1)
								replace updateVar=subinstr(updateVar, "`letter'`1'`2'.`3'-.`4'", "`letter'`1'`2'.`3'-`letter'`1'`2'.`4'",.)	
						}
						// Correct decimal range if the decimal is explicit (part 2)
						if `4' >= `1' {
							foreach 5 of numlist 0/9 {
									if `5' >= `2' replace updateVar=subinstr(updateVar, "`letter'`1'`2'.`3'-`4'`5'.", "`letter'`1'`2'.`3'-`letter'`4'`5'.",.) 	
								}
						}
					// Correct range only if the second element is greater or equal to the first
						if `3' > `1' | (`3' == `1' & `4' > `2') replace updateVar=subinstr(updateVar,"`letter'`1'`2'-`3'`4'","`letter'`1'`2'-`letter'`3'`4'", .) 
				}
			}
		}
	}	
	
	replace `causeVar' = updateVar if coding_system == "ICD10"	
	drop updateVar
	
** *****************************************************************************
** End format_ICD10_codes.do
** *****************************************************************************

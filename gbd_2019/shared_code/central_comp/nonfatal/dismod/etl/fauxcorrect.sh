etl_cause() {
	if [ "$#" -ne 2 ]
		then
  		echo "requires fauxcorrect version and cause_id"
  		return 1
	fi

	fauxcorrect_version=$1
	cause_id=$2

	target_dir="PATH"
	source_dir="PATH"

	target_file="${target_dir}/${cause_id}_${fauxcorrect_version}.csv"

	awk -F, '$6 == '${cause_id}' && $7 == 3' "${source_dir}"/{1990,1995,2000,2005,2010,2015,2017}/*.csv >> $target_file
}

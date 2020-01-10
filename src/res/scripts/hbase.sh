if [ -z ${1+x} ] ; then 
echo "user is unset"
exit 1;
fi

echo -e "disable 'spark_education:$1_html_data'" | hbase shell -n
echo -e "drop 'spark_education:$1_html_data'" | hbase shell -n
echo -e "create 'spark_education:$1_html_data', {NAME => 'html_content', VERSIONS => 10}" | hbase shell -n
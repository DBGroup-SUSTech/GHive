rm ~/plan/query_result_CPU.txt

echo "Q1.1: " >> ~/plan/query_result_CPU.txt
hive --database ssb_2_orc -e "source /home/hive/app/queries/ssb/Q1.1.sql;" >> ~/plan/query_result_CPU.txt

echo "Q1.2: " >> ~/plan/query_result_CPU.txt
hive --database ssb_2_orc -e "source /home/hive/app/queries/ssb/Q1.2.sql;" >> ~/plan/query_result_CPU.txt

echo "Q1.3: " >> ~/plan/query_result_CPU.txt
hive --database ssb_2_orc -e "source /home/hive/app/queries/ssb/Q1.3.sql;" >> ~/plan/query_result_CPU.txt

echo "Q1.4: " >> ~/plan/query_result_CPU.txt
hive --database ssb_2_orc -e "source /home/hive/app/queries/ssb/Q1.4.sql;" >> ~/plan/query_result_CPU.txt

echo "Q2.1: " >> ~/plan/query_result_CPU.txt
hive --database ssb_2_orc -e "source /home/hive/app/queries/ssb/Q2.1.sql;" >> ~/plan/query_result_CPU.txt

echo "Q2.2: " >> ~/plan/query_result_CPU.txt
hive --database ssb_2_orc -e "source /home/hive/app/queries/ssb/Q2.2.sql;" >> ~/plan/query_result_CPU.txt

echo "Q2.3: " >> ~/plan/query_result_CPU.txt
hive --database ssb_2_orc -e "source /home/hive/app/queries/ssb/Q2.3.sql;" >> ~/plan/query_result_CPU.txt

echo "Q3.1: " >> ~/plan/query_result_CPU.txt
hive --database ssb_2_orc -e "source /home/hive/app/queries/ssb/Q3.1.sql;" >> ~/plan/query_result_CPU.txt

echo "Q3.2: " >> ~/plan/query_result_CPU.txt
hive --database ssb_2_orc -e "source /home/hive/app/queries/ssb/Q3.2.sql;" >> ~/plan/query_result_CPU.txt

echo "Q3.3: " >> ~/plan/query_result_CPU.txt
hive --database ssb_2_orc -e "source /home/hive/app/queries/ssb/Q3.3.sql;" >> ~/plan/query_result_CPU.txt

echo "Q3.4: " >> ~/plan/query_result_CPU.txt
hive --database ssb_2_orc -e "source /home/hive/app/queries/ssb/Q3.4.sql;" >> ~/plan/query_result_CPU.txt

echo "Q4.1: " >> ~/plan/query_result_CPU.txt
hive --database ssb_2_orc -e "source /home/hive/app/queries/ssb/Q4.1.sql;" >> ~/plan/query_result_CPU.txt

echo "Q4.2: " >> ~/plan/query_result_CPU.txt
hive --database ssb_2_orc -e "source /home/hive/app/queries/ssb/Q4.2.sql;" >> ~/plan/query_result_CPU.txt

echo "Q4.3: " >> ~/plan/query_result_CPU.txt
hive --database ssb_2_orc -e "source /home/hive/app/queries/ssb/Q4.3.sql;" >> ~/plan/query_result_CPU.txt








#pattern="Stage-0"
#cat ssb_2_orc_1_1.txt | while read line
#do
#    if [[ $line =~ $pattern ]]
#    then
#        echo $line
#    fi
#done
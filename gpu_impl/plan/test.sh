rm ~/plan/query_result.txt


scp /tmp/ssb_2_orc_1_1.txt ~/plan/plan.txt
scp /tmp/ssb_2_orc_1_1_extended.txt ~/plan/plan_extended.txt
scp dbg19:/tmp/ssb_2_orc_1_1.txt dbg19:~/plan/plan.txt
scp dbg19:/tmp/ssb_2_orc_1_1_extended.txt dbg19:~/plan/plan_extended.txt
scp dbg16:/tmp/ssb_2_orc_1_1.txt dbg16:~/plan/plan.txt
scp dbg16:/tmp/ssb_2_orc_1_1_extended.txt dbg16:~/plan/plan_extended.txt
echo "Q1.1: " >> ~/plan/query_result.txt
hive --database ssb_2_orc -e "source /home/hive/app/queries/ssb/Q1.1.sql;" >> ~/plan/query_result.txt

scp /tmp/ssb_2_orc_1_2.txt ~/plan/plan.txt
scp /tmp/ssb_2_orc_1_2_extended.txt ~/plan/plan_extended.txt
scp dbg19:/tmp/ssb_2_orc_1_2.txt dbg19:~/plan/plan.txt
scp dbg19:/tmp/ssb_2_orc_1_2_extended.txt dbg19:~/plan/plan_extended.txt
scp dbg16:/tmp/ssb_2_orc_1_2.txt dbg16:~/plan/plan.txt
scp dbg16:/tmp/ssb_2_orc_1_2_extended.txt dbg16:~/plan/plan_extended.txt
echo "Q1.2: " >> ~/plan/query_result.txt
hive --database ssb_2_orc -e "source /home/hive/app/queries/ssb/Q1.2.sql;" >> ~/plan/query_result.txt

scp /tmp/ssb_2_orc_1_3.txt ~/plan/plan.txt
scp /tmp/ssb_2_orc_1_3_extended.txt ~/plan/plan_extended.txt
scp dbg19:/tmp/ssb_2_orc_1_3.txt dbg19:~/plan/plan.txt
scp dbg19:/tmp/ssb_2_orc_1_3_extended.txt dbg19:~/plan/plan_extended.txt
scp dbg16:/tmp/ssb_2_orc_1_3.txt dbg16:~/plan/plan.txt
scp dbg16:/tmp/ssb_2_orc_1_3_extended.txt dbg16:~/plan/plan_extended.txt
echo "Q1.3: " >> ~/plan/query_result.txt
hive --database ssb_2_orc -e "source /home/hive/app/queries/ssb/Q1.3.sql;" >> ~/plan/query_result.txt


scp /tmp/ssb_2_orc_1_4.txt ~/plan/plan.txt
scp /tmp/ssb_2_orc_1_4_extended.txt ~/plan/plan_extended.txt
scp dbg19:/tmp/ssb_2_orc_1_4.txt dbg19:~/plan/plan.txt
scp dbg19:/tmp/ssb_2_orc_1_4_extended.txt dbg19:~/plan/plan_extended.txt
scp dbg16:/tmp/ssb_2_orc_1_4.txt dbg16:~/plan/plan.txt
scp dbg16:/tmp/ssb_2_orc_1_4_extended.txt dbg16:~/plan/plan_extended.txt
echo "Q1.4: " >> ~/plan/query_result.txt
hive --database ssb_2_orc -e "source /home/hive/app/queries/ssb/Q1.4.sql;" >> ~/plan/query_result.txt


scp /tmp/ssb_2_orc_2_1.txt ~/plan/plan.txt
scp /tmp/ssb_2_orc_2_1_extended.txt ~/plan/plan_extended.txt
scp dbg19:/tmp/ssb_2_orc_2_1.txt dbg19:~/plan/plan.txt
scp dbg19:/tmp/ssb_2_orc_2_1_extended.txt dbg19:~/plan/plan_extended.txt
scp dbg16:/tmp/ssb_2_orc_2_1.txt dbg16:~/plan/plan.txt
scp dbg16:/tmp/ssb_2_orc_2_1_extended.txt dbg16:~/plan/plan_extended.txt
echo "Q2.1: " >> ~/plan/query_result.txt
hive --database ssb_2_orc -e "source /home/hive/app/queries/ssb/Q2.1.sql;" >> ~/plan/query_result.txt


scp /tmp/ssb_2_orc_2_2.txt ~/plan/plan.txt
scp /tmp/ssb_2_orc_2_2_extended.txt ~/plan/plan_extended.txt
scp dbg19:/tmp/ssb_2_orc_2_2.txt dbg19:~/plan/plan.txt
scp dbg19:/tmp/ssb_2_orc_2_2_extended.txt dbg19:~/plan/plan_extended.txt
scp dbg16:/tmp/ssb_2_orc_2_2.txt dbg16:~/plan/plan.txt
scp dbg16:/tmp/ssb_2_orc_2_2_extended.txt dbg16:~/plan/plan_extended.txt
echo "Q2.2: " >> ~/plan/query_result.txt
hive --database ssb_2_orc -e "source /home/hive/app/queries/ssb/Q2.2.sql;" >> ~/plan/query_result.txt

scp /tmp/ssb_2_orc_2_3.txt ~/plan/plan.txt
scp /tmp/ssb_2_orc_2_3_extended.txt ~/plan/plan_extended.txt
scp dbg19:/tmp/ssb_2_orc_2_3.txt dbg19:~/plan/plan.txt
scp dbg19:/tmp/ssb_2_orc_2_3_extended.txt dbg19:~/plan/plan_extended.txt
scp dbg16:/tmp/ssb_2_orc_2_3.txt dbg16:~/plan/plan.txt
scp dbg16:/tmp/ssb_2_orc_2_3_extended.txt dbg16:~/plan/plan_extended.txt
echo "Q2.3: " >> ~/plan/query_result.txt
hive --database ssb_2_orc -e "source /home/hive/app/queries/ssb/Q2.3.sql;" >> ~/plan/query_result.txt

scp /tmp/ssb_2_orc_3_1.txt ~/plan/plan.txt
scp /tmp/ssb_2_orc_3_1_extended.txt ~/plan/plan_extended.txt
scp dbg19:/tmp/ssb_2_orc_3_1.txt dbg19:~/plan/plan.txt
scp dbg19:/tmp/ssb_2_orc_3_1_extended.txt dbg19:~/plan/plan_extended.txt
scp dbg16:/tmp/ssb_2_orc_3_1.txt dbg16:~/plan/plan.txt
scp dbg16:/tmp/ssb_2_orc_3_1_extended.txt dbg16:~/plan/plan_extended.txt
echo "Q3.1: " >> ~/plan/query_result.txt
hive --database ssb_2_orc -e "source /home/hive/app/queries/ssb/Q3.1.sql;" >> ~/plan/query_result.txt

scp /tmp/ssb_2_orc_3_2.txt ~/plan/plan.txt
scp /tmp/ssb_2_orc_3_2_extended.txt ~/plan/plan_extended.txt
scp dbg19:/tmp/ssb_2_orc_3_2.txt dbg19:~/plan/plan.txt
scp dbg19:/tmp/ssb_2_orc_3_2_extended.txt dbg19:~/plan/plan_extended.txt
scp dbg16:/tmp/ssb_2_orc_3_2.txt dbg16:~/plan/plan.txt
scp dbg16:/tmp/ssb_2_orc_3_2_extended.txt dbg16:~/plan/plan_extended.txt
echo "Q3.2: " >> ~/plan/query_result.txt
hive --database ssb_2_orc -e "source /home/hive/app/queries/ssb/Q3.2.sql;" >> ~/plan/query_result.txt

scp /tmp/ssb_2_orc_3_3.txt ~/plan/plan.txt
scp /tmp/ssb_2_orc_3_3_extended.txt ~/plan/plan_extended.txt
scp dbg19:/tmp/ssb_2_orc_3_3.txt dbg19:~/plan/plan.txt
scp dbg19:/tmp/ssb_2_orc_3_3_extended.txt dbg19:~/plan/plan_extended.txt
scp dbg16:/tmp/ssb_2_orc_3_3.txt dbg16:~/plan/plan.txt
scp dbg16:/tmp/ssb_2_orc_3_3_extended.txt dbg16:~/plan/plan_extended.txt
echo "Q3.3: " >> ~/plan/query_result.txt
hive --database ssb_2_orc -e "source /home/hive/app/queries/ssb/Q3.3.sql;" >> ~/plan/query_result.txt

scp /tmp/ssb_2_orc_3_4.txt ~/plan/plan.txt
scp /tmp/ssb_2_orc_3_4_extended.txt ~/plan/plan_extended.txt
scp dbg19:/tmp/ssb_2_orc_3_4.txt dbg19:~/plan/plan.txt
scp dbg19:/tmp/ssb_2_orc_3_4_extended.txt dbg19:~/plan/plan_extended.txt
scp dbg16:/tmp/ssb_2_orc_3_4.txt dbg16:~/plan/plan.txt
scp dbg16:/tmp/ssb_2_orc_3_4_extended.txt dbg16:~/plan/plan_extended.txt
echo "Q3.4: " >> ~/plan/query_result.txt
hive --database ssb_2_orc -e "source /home/hive/app/queries/ssb/Q3.4.sql;" >> ~/plan/query_result.txt

scp /tmp/ssb_2_orc_4_1.txt ~/plan/plan.txt
scp /tmp/ssb_2_orc_4_1_extended.txt ~/plan/plan_extended.txt
scp dbg19:/tmp/ssb_2_orc_4_1.txt dbg19:~/plan/plan.txt
scp dbg19:/tmp/ssb_2_orc_4_1_extended.txt dbg19:~/plan/plan_extended.txt
scp dbg16:/tmp/ssb_2_orc_4_1.txt dbg16:~/plan/plan.txt
scp dbg16:/tmp/ssb_2_orc_4_1_extended.txt dbg16:~/plan/plan_extended.txt
echo "Q4.1: " >> ~/plan/query_result.txt
hive --database ssb_2_orc -e "source /home/hive/app/queries/ssb/Q4.1.sql;" >> ~/plan/query_result.txt

scp /tmp/ssb_2_orc_4_2.txt ~/plan/plan.txt
scp /tmp/ssb_2_orc_4_2_extended.txt ~/plan/plan_extended.txt
scp dbg19:/tmp/ssb_2_orc_4_2.txt dbg19:~/plan/plan.txt
scp dbg19:/tmp/ssb_2_orc_4_2_extended.txt dbg19:~/plan/plan_extended.txt
scp dbg16:/tmp/ssb_2_orc_4_2.txt dbg16:~/plan/plan.txt
scp dbg16:/tmp/ssb_2_orc_4_2_extended.txt dbg16:~/plan/plan_extended.txt
echo "Q4.2: " >> ~/plan/query_result.txt
hive --database ssb_2_orc -e "source /home/hive/app/queries/ssb/Q4.2.sql;" >> ~/plan/query_result.txt


scp /tmp/ssb_2_orc_4_3.txt ~/plan/plan.txt
scp /tmp/ssb_2_orc_4_3_extended.txt ~/plan/plan_extended.txt
scp dbg19:/tmp/ssb_2_orc_4_3.txt dbg19:~/plan/plan.txt
scp dbg19:/tmp/ssb_2_orc_4_3_extended.txt dbg19:~/plan/plan_extended.txt
scp dbg16:/tmp/ssb_2_orc_4_3.txt dbg16:~/plan/plan.txt
scp dbg16:/tmp/ssb_2_orc_4_3_extended.txt dbg16:~/plan/plan_extended.txt
echo "Q4.3: " >> ~/plan/query_result.txt
hive --database ssb_2_orc -e "source /home/hive/app/queries/ssb/Q4.3.sql;" >> ~/plan/query_result.txt








#pattern="Stage-0"
#cat ssb_2_orc_1_1.txt | while read line
#do
#    if [[ $line =~ $pattern ]]
#    then
#        echo $line
#    fi
#done
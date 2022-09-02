10만건 bulk insert

elapsed time : 62.19419 sec
deleted values cnt : 108841
read df elapsed time : 1.25661 sec
108841
add all features elapsed time : 8.14932 sec
total elapsed time : 24.27404 sec
total_success_cnt : 108841



60K씩 잘라서 piepelining

elapsed time : 111.73861 sec
deleted values cnt : 108841
read df elapsed time : 1.72354 sec
108841
total elapsed time : 105.25976 sec
total_success_cnt : 108841



2398000건  bulk insert

total elapsed time : 630.81953 sec
total_success_cnt : 2398000


시리얼라이징만 마지막으로 해보자

serializing
golang
분산처리



feast에서는
redis저장할때 entity(key)를 serialization하여 만듬
pay_account_id  102010 yyyymmdd 20220701 project-yms
value들은 hset으로 만든후  key에 mapping하여 삽입
murmur3_32 hash 


snappy 로 압축한이후에 용량은 1/3 , 적재속도도 많이 올라감.

--> 용량크면 그 자체로 적재속도에 영향을 미치는듯


https://data-engineer-tech.tistory.com/35

total elapsed time : 340.05990 sec
total_success_cnt : 2398000



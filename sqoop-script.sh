#!/bin/bash

OPTIONS_FILE="/home/test/sqoop-credential"
CURRENT_DATE=$(date +"%Y-%m-%d")
HDFS_PATH="/user/spark/vedant/project_data/"

TABLES=("user_data" "card_data" "transaction_data")

for TABLE_NAME in "${TABLES[@]}"; do
  echo "$(date '+%Y-%m-%d %H:%M:%S') - Starting Sqoop import for table: $TABLE_NAME"

  if [ "$TABLE_NAME" == "user_data" ]; then
    sqoop import \
      --options-file "$OPTIONS_FILE" \
      --driver com.mysql.cj.jdbc.Driver \
      --table "$TABLE_NAME" \
      --columns "id,current_age,retirement_age,birth_year,birth_month,gender,address,latitude,longitude,per_capita_income,yearly_income,total_debt,credit_score,num_credit_cards" \
      --map-column-java id=Integer,current_age=Integer,retirement_age=Integer,birth_year=Integer,birth_month=Integer,gender=String,address=String,latitude=Double,longitude=Double,per_capita_income=String,yearly_income=String,total_debt=String,credit_score=Integer,num_credit_cards=Integer \
      --target-dir "$HDFS_PATH/$CURRENT_DATE/$TABLE_NAME" \
      --as-parquetfile \
      --fields-terminated-by ',' \
      --lines-terminated-by '\n' \
      --num-mappers 2 \
      --delete-target-dir
  elif [ "$TABLE_NAME" == "card_data" ]; then
    sqoop import \
      --options-file "$OPTIONS_FILE" \
      --driver com.mysql.cj.jdbc.Driver \
      --table "$TABLE_NAME" \
      --columns "id,client_id,card_brand,card_type,card_number,expires,cvv,has_chip,num_cards_issued,credit_limit,acct_open_date,year_pin_last_changed,card_on_dark_web" \
      --map-column-java id=Integer,client_id=Integer,card_brand=String,card_type=String,card_number=Long,expires=String,cvv=Integer,has_chip=String,num_cards_issued=Integer,credit_limit=String,acct_open_date=String,year_pin_last_changed=Integer,card_on_dark_web=String \
      --target-dir "$HDFS_PATH/$CURRENT_DATE/$TABLE_NAME" \
      --as-parquetfile \
      --fields-terminated-by ',' \
      --lines-terminated-by '\n' \
      --num-mappers 1 \
      --delete-target-dir
  elif [ "$TABLE_NAME" == "transaction_data" ]; then
    sqoop import \
      --options-file "$OPTIONS_FILE" \
      --driver com.mysql.cj.jdbc.Driver \
      --table "$TABLE_NAME" \
      --columns "id,date,client_id,card_id,amount,use_chip,merchant_id,merchant_city,merchant_state,zip,mcc,errors" \
      --map-column-java id=Integer,date=String,client_id=Integer,card_id=Integer,amount=String,use_chip=String,merchant_id=Integer,merchant_city=String,merchant_state=String,zip=Double,mcc=Integer,errors=String \
      --target-dir "$HDFS_PATH/$CURRENT_DATE/$TABLE_NAME" \
      --as-parquetfile \
      --fields-terminated-by ',' \
      --lines-terminated-by '\n' \
      --num-mappers 1 \
      --delete-target-dir
  fi

  if [ $? -eq 0 ]; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Sqoop import completed successfully for table: $TABLE_NAME (as parquetfile format)"
  else
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Sqoop import failed for table: $TABLE_NAME. Check the logs for details."
    exit 2
  fi

  echo "$(date '+%Y-%m-%d %H:%M:%S') - Sqoop import for \"$TABLE_NAME\" completed."
done

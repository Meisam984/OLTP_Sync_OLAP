build:
	docker pull samrez84/hadoop-spark-lab
	docker build -t hadoop-hive-spark-master ./master
	docker build -t hadoop-hive-spark-worker ./worker
	docker build -t hadoop-hive-spark-history ./history
	docker pull samrez84/hadoop-spark-lab-jupyter
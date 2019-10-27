# Financial_BD
金融大数据作业4

1、矩阵乘法：MatrixMultiply
2、关系代数：RelationAlgebra

具体操作请见：金融大数据-作业4.md/pdf,内附每个问题的操作步骤、参数设置、注意事项。
所有运行结果截图在“运行结果截图”文件夹中，每个项目文件夹的java目录下也有所有生成的part-r-00000输出文件。


#####大纲preview：
### 1、矩阵乘法
##### 项目名称：MatrixMultiply
hdfs://localhost:9000/input1/M_3_4
hdfs://localhost:9000/input2/N_4_2
hdfs://localhost:9000/output/

### 2、关系代数
##### 项目名称：RelationAlgebra

#### 2.1 选择
在Ra.txt上选择age=18的记录；在Ra.txt上选择age<18的记录 。
hdfs://localhost:9000/inputRa/
hdfs://localhost:9000/output/
2
18

**age=18和<18**的情况下，**RelationA.java**文件的判断条件也不同。
//line36
public boolean isCondition(int col, String value){
		if(col == 0 && Integer.parseInt(value) == this.id)
			return true;
		else if(col == 1 && name.equals(value))
			return true;
		else if(col ==2 && Integer.parseInt(value) == this.age)//age<18时请改成大于符号
			return true;


#### 2.2 投影
hdfs://localhost:9000/inputRa/
hdfs://localhost:9000/output/
1

#### 2.3 并集
hdfs://localhost:9000/inputRa1/
hdfs://localhost:9000/output/

#### 2.4 交集
hdfs://localhost:9000/inputRa1/
hdfs://localhost:9000/output/

#### 2.5 差
hdfs://localhost:9000/inputRa1/
hdfs://localhost:9000/output/
Ra2.txt

#### 2.6 自然连接
hdfs://localhost:9000/inputRab/
hdfs://localhost:9000/output/
0
Ra.txt

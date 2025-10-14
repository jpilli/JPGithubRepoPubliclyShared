# Databricks notebook source
# MAGIC %md
# MAGIC #PySpark Joins - Join types explained

# COMMAND ----------

# MAGIC %md
# MAGIC ##Syntax - dataframe join
# MAGIC
# MAGIC `dataframe.join(other, on=None, how=None)`
# MAGIC
# MAGIC **Parameters:**
# MAGIC
# MAGIC   *other* : Other dataframe, which is the RIGHT side of the join
# MAGIC
# MAGIC   *on* : Which column(s) to join ON?
# MAGIC
# MAGIC   *how* : Join type as string. Default INNER. 
# MAGIC
# MAGIC **Returns:** Joined dataframe.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##Example dataframes
# MAGIC

# COMMAND ----------

#1. Employee dataframe
df_employee = spark.createDataFrame(
    [
        ('E1001','Kevin','D501'),
        ('E1002','David','D502'),
        ('E1003','Ben','D502'),
        ('E1004','Linda','D503'),
        ('E1005','Olivia','D504') 
    ],
    "EmpID: string, Name: string, DeptID: string"
)

df_employee.display()

#2. Department dataframe
df_department = spark.createDataFrame(
    [
        ('D501','Engineering'),
        ('D502','Sales'),
        ('D503','Marketing'),
        ('D525','IT') 
    ],
    "DeptID: string, Name: string"
)

df_department.display()

#3. Location dataframe
df_location = spark.createDataFrame(
    [
        ('SYD','Sydney'),
        ('MEL','Melbourne'),
        ('ADL','Adelaide') 
    ],
    "CityCode: string, Name: string"
)

df_location.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ##Join Types - Basic Examples

# COMMAND ----------

# MAGIC %md
# MAGIC ###1.0 INNER JOIN (default)
# MAGIC
# MAGIC An **inner join** returns only the rows that have matching keys (in this example, DeptID) in both the left and right dataframes.
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col

df_emp = df_employee.alias("df_emp")
df_dept = df_department.alias("df_dept")

df_jointype_inner = (
        df_emp
        .join(df_dept,
            df_emp.DeptID == df_dept.DeptID,
            "inner" #default join type: inner
        )
        .select(
            df_emp.EmpID.alias("EmployeeID"),
            df_emp.Name.alias("EmployeeName"),
            df_emp.DeptID.alias("DeptID_AsInLeftDf"),
            df_dept.DeptID.alias("DeptID_AsInRightDf"),
            df_dept.Name.alias("DepartmentName")
            )
        .sort(col("EmployeeID").asc())
    )

df_jointype_inner.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###2.0 LEFT OUTER JOIN
# MAGIC
# MAGIC A left outer join returns all rows from the left dataframe, along with matching rows from the right dataframe. If no match is found, the corresponding columns from the right dataframe are filled with NULL.
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col

df_emp = df_employee.alias("df_emp")
df_dept = df_department.alias("df_dept")

df_jointype_leftouter = (
        df_emp
        .join(df_dept,
            df_emp.DeptID == df_dept.DeptID,
            "leftouter" #Must be one of: left, leftouter, left_outer, 
        )
        .select(
            df_emp.EmpID.alias("EmployeeID"),
            df_emp.Name.alias("EmployeeName"),
            df_emp.DeptID.alias("DeptID_AsInLeftDf"),
            df_dept.DeptID.alias("DeptID_AsInRightDf"),
            df_dept.Name.alias("DepartmentName")
            )
        .sort(col("EmployeeID").asc_nulls_last())
    )

df_jointype_leftouter.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###3.0 RIGHT OUTER JOIN
# MAGIC
# MAGIC A right outer join returns all rows from the right dataframe, along with matching rows from the left dataframe. If no match is found, the corresponding columns from the left dataframe are filled with NULL.
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col

df_emp = df_employee.alias("df_emp")
df_dept = df_department.alias("df_dept")

df_jointype_rightouter = (
        df_emp
        .join(df_dept,
            df_emp.DeptID == df_dept.DeptID,
            "rightouter" #Must be one of: right, rightouter, right_outer
        )
        .select(
            df_emp.EmpID.alias("EmployeeID"),
            df_emp.Name.alias("EmployeeName"),
            df_emp.DeptID.alias("DeptID_AsInLeftDf"),
            df_dept.DeptID.alias("DeptID_AsInRightDf"),
            df_dept.Name.alias("DepartmentName")
            )
        .sort(col("EmployeeID").asc_nulls_last())
    )

df_jointype_rightouter.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###4.0 FULL OUTER JOIN
# MAGIC
# MAGIC A full outer join returns all rows from both the left and right dataframes. If a row has no match on the other side, the corresponding columns are filled with NULL.

# COMMAND ----------

from pyspark.sql.functions import col

df_emp = df_employee.alias("df_emp")
df_dept = df_department.alias("df_dept")

df_jointype_fullouter = (
        df_emp
        .join(df_dept,
            df_emp.DeptID == df_dept.DeptID,
            "fullouter" #Must be one of: outer, full, fullouter, full_outer
        )
        .select(
            df_emp.EmpID.alias("EmployeeID"),
            df_emp.Name.alias("EmployeeName"),
            df_emp.DeptID.alias("DeptID_AsInLeftDf"),
            df_dept.DeptID.alias("DeptID_AsInRightDf"),
            df_dept.Name.alias("DepartmentName")
            )
        .sort(col("EmployeeID").asc_nulls_last())
    )

df_jointype_fullouter.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###5.0 LEFT SEMI JOIN
# MAGIC
# MAGIC A left semi join returns only the rows from the left dataframe that have a match in the right dataframe. Unlike other joins, columns from the right dataframe are not accessible.
# MAGIC
# MAGIC **ATTENTION**: When using a left semi (or left anti) join, do not attempt to query columns from the right dataframe, as this may result in errors or incorrect values.

# COMMAND ----------

from pyspark.sql.functions import col

df_emp = df_employee.alias("df_emp")
df_dept = df_department.alias("df_dept")

#1.0 Let's first inspect the schema of the leftsemi joined dataframe:
df_jointype_leftsemi = (
        df_emp
        .join(df_dept,
            df_emp.DeptID == df_dept.DeptID,
            "leftsemi" #Must be one of: semi, leftsemi, left_semi
        )
)

#1.1 In a left semi/anti join, columns in RIGHT dataframe aren't available to query, as you can see in the printSchema() as below
df_jointype_leftsemi.printSchema()

# root
#  |-- EmpID: string (nullable = true)
#  |-- Name: string (nullable = true)
#  |-- DeptID: string (nullable = true)


#2.0 Let's query the leftsemi joined dataframe
df_jointype_leftsemi = (
        df_emp
        .join(df_dept,
            df_emp.DeptID == df_dept.DeptID,
            "leftsemi" #Must be one of: semi, leftsemi, left_semi
        )
        .select(
            df_emp.EmpID.alias("EmployeeID"),
            df_emp.Name.alias("EmployeeName"),
            df_emp.DeptID.alias("DeptID_AsInLeftDf")

            #Tip: In a leftsemi/leftanti join, columns in RIGHT dataframe aren't available to query. If you still try to query them:
            # (i) if the column name matches with that in left dataframe, then that column values would be shown, which is incorrect. 
            # (ii) if the column name specified is in right df, but not in left dataframe, you'd get an error.

            # df_dept.DeptID.alias("DeptID_AsInRightDf"),
            # df_dept.Name.alias("DepartmentName") #Incorrect values (EmployeeName) shown
            )
        .sort(col("EmployeeID").asc())
    )

df_jointype_leftsemi.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###6.0 LEFT ANTI JOIN
# MAGIC
# MAGIC A left anti join returns only the rows from the left dataframe that have no match in the right dataframe. Like a left semi join, columns from the right dataframe are not accessible.
# MAGIC
# MAGIC **ATTENTION**: When using a left semi (or left anti) join, do not attempt to query columns from the right dataframe, as this may result in errors or incorrect values.

# COMMAND ----------

from pyspark.sql.functions import col

df_emp = df_employee.alias("df_emp")
df_dept = df_department.alias("df_dept")

#1.0 Let's first inspect the schema of the leftanti joined dataframe:
df_jointype_leftanti = (
        df_emp
        .join(df_dept,
            df_emp.DeptID == df_dept.DeptID,
            "leftanti" #Must be one of: anti, leftanti and left_anti
        )
)

#1.1 In a left semi/anti join, columns in RIGHT dataframe aren't available to query, as you can see in the printSchema() as below
df_jointype_leftanti.printSchema()

# root
#  |-- EmpID: string (nullable = true)
#  |-- Name: string (nullable = true)
#  |-- DeptID: string (nullable = true)

#2.0 Let's query the leftanti joined dataframe
df_jointype_leftanti = (
        df_emp
        .join(df_dept,
            df_emp.DeptID == df_dept.DeptID,
            "leftanti" #Must be one of: anti, leftanti and left_anti
        )
        .select(
            df_emp.EmpID.alias("EmployeeID"),
            df_emp.Name.alias("EmployeeName"),
            df_emp.DeptID.alias("DeptID_AsInLeftDf")

            #Tip: In a leftsemi/leftanti join, columns in RIGHT dataframe aren't available to query. If you still try to query them:
            # (i) if the column name matches with that in left dataframe, then that column values would be shown, which is incorrect. 
            # (ii) if the column name specified is in right df, but not in left dataframe, you'd get an error.

            # df_dept.DeptID.alias("DeptID_AsInRightDf"),
            # df_dept.Name.alias("DepartmentName") #Incorrect values (EmployeeName) shown
            )
        .sort(col("EmployeeID").asc())
    )

df_jointype_leftanti.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###7.0 CROSS JOIN
# MAGIC
# MAGIC A cross join combines every row from one dataframe with every row from the other, producing the Cartesian product of the two dataframes.
# MAGIC
# MAGIC In our example as below, the Department dataframe has 4 rows, and the Location dataframe has 3 rows. Applying a cross join produces 12 rows in total (4 × 3).
# MAGIC
# MAGIC **Caution**: In production, use cross joins carefully. The output size can grow very quickly. If one dataframe has N rows and the other has M rows, the result will have N × M rows. For example, joining a 10 million-row dataframe with even a tiny 10-row dataframe results in 100 million rows.
# MAGIC
# MAGIC *Tip*: When performing a cross join in PySpark, do not specify columns in a join condition. Instead, set the join condition to None.

# COMMAND ----------

from pyspark.sql.functions import col

df_dept = df_department.alias("df_dept")
df_loc = df_location.alias("df_loc")

#Let's do a CROSS join between df_department and df_location
df_jointype_cross = (
        df_dept
        .join(df_loc,
            None, #In case of a cross join, don't specify any columns to join on. Instead, the join condition must be None. 
            "cross" 
        )
        .select(
            df_dept.DeptID.alias("DeptID"),
            df_dept.Name.alias("DepartmentName"),
            df_loc.CityCode.alias("CityCode"),
            df_loc.Name.alias("CityName")
            )
        .sort(col("DeptID").asc())
    )

df_jointype_cross.display()

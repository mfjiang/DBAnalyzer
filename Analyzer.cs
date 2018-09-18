using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.SqlClient;

//author    江名峰

namespace DBAnalyzer
{
    /// <summary>
    /// 表示数据库分析对象类型
    /// </summary>
    internal class Analyzer
    {
        #region 字段
        private SqlConnection master;
        private SqlConnection devison;
        private DataTable results;

        private List<ResultInfo> lrs = new List<ResultInfo>();
        #endregion

        /// <summary>
        /// 构造经初始化数据库连接的实例
        /// </summary>
        /// <param name="masterConn">主数据库连接串</param>
        /// <param name="devisonConn">分数据库连接串</param>
        public Analyzer(string masterConn, string devisonConn)
        {
            if (String.IsNullOrEmpty(masterConn)) { throw new ArgumentNullException("masterConn"); }
            if (String.IsNullOrEmpty(devisonConn)) { throw new ArgumentNullException("devisonConn"); }

            try
            {
                master = new SqlConnection(masterConn);
            }
            catch (Exception ex)
            {
                throw new Exception("无法创建到主数据库的连接", ex);
            }

            try
            {
                devison = new SqlConnection(devisonConn);
            }
            catch (Exception ex)
            {
                throw new Exception("无法创建到分数据库的连接", ex);
            }

            //初始化结果表
            results = new DataTable("Results");

            DataColumn itemNameColumn = new DataColumn("ItemName");
            DataColumn masterValueColumn = new DataColumn("Master");
            DataColumn divisionValueColumn = new DataColumn("Division");

            results.Columns.Add(itemNameColumn);
            results.Columns.Add(masterValueColumn);
            results.Columns.Add(divisionValueColumn);
        }

        #region 方法
        /// <summary>
        /// 分析数据库
        /// </summary>
        /// <returns>分析结果</returns>
        public DataTable Analyze()
        {
            if (master != null && devison != null)
            {
                //分析步骤:
                //统计双方表数目
                lrs.Add(CountTable(master, devison));

                //统计双方存储过程数目
                lrs.Add(CountSp(master, devison));

                //缺少的表                
                lrs.AddRange(MissedTable(master, devison));

                //缺少的存储过程
                lrs.AddRange(MissedSp(master, devison));

                //缺少的列,变更的列
                lrs.AddRange(ColumnChanages(master, devison));

                //缺少的键
                lrs.AddRange(MissedKeyConstraint(master, devison));

            }

            if (lrs.Count > 0)
            {
                foreach (ResultInfo r in lrs)
                {
                    DataRow dr = results.NewRow();
                    dr[0] = r.ItemName;
                    dr[1] = r.MasterValue;
                    dr[2] = r.DevisonValue;
                    results.Rows.Add(dr);
                }
            }

            return results;
        }

        /// <summary>
        /// 比较表个数
        /// </summary>
        /// <param name="masterConn">主库连接</param>
        /// <param name="devisonConn">分库连接</param>
        /// <returns>结果信息</returns>
        private ResultInfo CountTable(SqlConnection masterConn, SqlConnection devisonConn)
        {
            SqlCommand command = new SqlCommand();
            command.Connection = masterConn;
            command.CommandText = "select count(*) from INFORMATION_SCHEMA.TABLES";

            masterConn.Open();
            string totalMasterTables = command.ExecuteScalar().ToString();
            masterConn.Close();

            command.Connection = devisonConn;
            devisonConn.Open();
            string totalDevisonTables = command.ExecuteScalar().ToString();
            devisonConn.Close();

            return new ResultInfo("表个数", totalMasterTables, totalDevisonTables);
        }

        /// <summary>
        /// 比较存储过程个数
        /// </summary>
        /// <param name="masterConn">主库连接</param>
        /// <param name="devisonConn">分库连接</param>
        /// <returns>结果信息</returns>
        private ResultInfo CountSp(SqlConnection masterConn, SqlConnection devisonConn)
        {
            SqlCommand command = new SqlCommand();
            SqlParameter p1 = new SqlParameter("@sp_owner", "dbo");
            command.CommandText = "sp_stored_procedures";
            command.Parameters.Add(p1);
            command.CommandType = CommandType.StoredProcedure;

            SqlDataReader sdr;

            command.Connection = masterConn;
            masterConn.Open();
            sdr = command.ExecuteReader();
            int totalMasterSp = 0;
            while (sdr.Read())
            {
                totalMasterSp += 1;
            }
            sdr.Close();
            masterConn.Close();

            command.Connection = devisonConn;
            devisonConn.Open();
            int totalDevisonSp = 0;
            sdr = command.ExecuteReader();
            while (sdr.Read())
            {
                totalDevisonSp += 1;
            }
            sdr.Close();
            devisonConn.Close();

            return new ResultInfo("存储过程个数", totalMasterSp.ToString(), totalDevisonSp.ToString());
        }

        /// <summary>
        /// 查找出所有缺失的表
        /// </summary>
        /// <param name="masterConn">主库连接</param>
        /// <param name="devisonConn">分库连接</param>
        //// <returns>结果信息</returns>
        private List<ResultInfo> MissedTable(SqlConnection masterConn, SqlConnection devisonConn)
        {
            List<ResultInfo> l = new List<ResultInfo>();

            List<String> masterTbNames = new List<String>();
            List<String> devisonTbNames = new List<String>();

            SqlDataReader sdr;
            SqlCommand command = new SqlCommand();

            command.Connection = masterConn;
            command.CommandText = "select * from INFORMATION_SCHEMA.TABLES";
            masterConn.Open();
            sdr = command.ExecuteReader();
            if (sdr.HasRows)
            {
                while (sdr.Read())
                {
                    masterTbNames.Add("[" + sdr["TABLE_NAME"].ToString() + "]");
                }
            }
            masterConn.Close();

            command.Connection = devisonConn;
            devisonConn.Open();
            sdr = command.ExecuteReader();
            if (sdr.HasRows)
            {
                while (sdr.Read())
                {
                    devisonTbNames.Add("[" + sdr["TABLE_NAME"].ToString() + "]");
                }
            }
            devisonConn.Close();

            foreach (string table in masterTbNames)
            {
                if (!devisonTbNames.Contains(table))
                {
                    l.Add(new ResultInfo("缺少的表", table, "缺"));
                }
            }

            return l;
        }

        /// <summary>
        /// 查找出所有缺失的存储过程
        /// </summary>
        /// <param name="masterConn">主库连接</param>
        /// <param name="devisonConn">分库连接</param>
        /// <returns>结果信息</returns>
        private List<ResultInfo> MissedSp(SqlConnection masterConn, SqlConnection devisonConn)
        {
            List<ResultInfo> l = new List<ResultInfo>();

            List<String> masterTbNames = new List<String>();
            List<String> devisonTbNames = new List<String>();

            SqlDataReader sdr;
            SqlCommand command = new SqlCommand();
            command.CommandText = "sp_stored_procedures";
            command.CommandType = CommandType.StoredProcedure;

            command.Connection = masterConn;

            masterConn.Open();
            sdr = command.ExecuteReader();
            if (sdr.HasRows)
            {
                while (sdr.Read())
                {
                    masterTbNames.Add("[" + sdr["PROCEDURE_NAME"].ToString() + "]");
                }
            }
            masterConn.Close();

            command.Connection = devisonConn;
            devisonConn.Open();
            sdr = command.ExecuteReader();
            if (sdr.HasRows)
            {
                while (sdr.Read())
                {
                    devisonTbNames.Add("[" + sdr["PROCEDURE_NAME"].ToString() + "]");
                }
            }
            devisonConn.Close();

            foreach (string table in masterTbNames)
            {
                if (!devisonTbNames.Contains(table))
                {
                    l.Add(new ResultInfo("缺少的存储过程", table, "缺"));
                }
            }

            return l;
        }

        /// <summary>
        /// 查找所有缺少的列和变更的列
        /// </summary>
        /// <param name="masterConn">主库连接</param>
        /// <param name="devisonConn">分库连接</param>
        /// <returns>结果信息</returns>
        private List<ResultInfo> ColumnChanages(SqlConnection masterConn, SqlConnection devisonConn)
        {
            List<ResultInfo> l = new List<ResultInfo>();

            List<String> tempColumns = new List<String>();
            List<String> tempColumnSettings = new List<String>();

            DataSet columnsSet = new DataSet("Columns");
            columnsSet.Tables.Add(new DataTable("Master"));
            columnsSet.Tables.Add(new DataTable("Devison"));

            SqlDataAdapter sda = new SqlDataAdapter();
            SqlCommand command = new SqlCommand();
            command.CommandText = "select * from INFORMATION_SCHEMA.COLUMNS";
            sda.SelectCommand = command;

            //分别读取数据
            command.Connection = masterConn;
            masterConn.Open();
            sda.Fill(columnsSet.Tables["Master"]);
            masterConn.Close();

            command.Connection = devisonConn;
            devisonConn.Open();
            sda.Fill(columnsSet.Tables["Devison"]);
            devisonConn.Close();

            //缺少的列

            //为便于查找将Devison表的列全名、列属性存成字符串
            foreach (DataRow r in columnsSet.Tables["Devison"].Rows)
            {
                tempColumns.Add("[" + r["TABLE_NAME"].ToString() + "].[" + r["COLUMN_NAME"] + "]");
                tempColumnSettings.Add("[" + r["TABLE_NAME"].ToString() + "].[" + r["COLUMN_NAME"] + "]"
                                           + "(可空:" + r["IS_NULLABLE"].ToString()
                                           + ",数据类型:" + r["DATA_TYPE"].ToString()
                                           + ",默认值:" + r["COLUMN_DEFAULT"]
                                           + ",最大字符长度:" + r["CHARACTER_MAXIMUM_LENGTH"].ToString()
                                           + ",数值精度:" + r["NUMERIC_PRECISION"].ToString()
                                           + ",数值精度基数:" + r["NUMERIC_PRECISION_RADIX"].ToString()
                                           + ")");
            }

            if (columnsSet.Tables["Master"].Rows.Count > 0)
            {
                foreach (DataRow r in columnsSet.Tables["Master"].Rows)
                {
                    string temp = "[" + r["TABLE_NAME"].ToString() + "].[" + r["COLUMN_NAME"] + "]";
                    string temp2 = "[" + r["TABLE_NAME"].ToString() + "].[" + r["COLUMN_NAME"] + "]"
                                           + "(可空:" + r["IS_NULLABLE"].ToString()
                                           + ",数据类型:" + r["DATA_TYPE"].ToString()
                                           + ",默认值:" + r["COLUMN_DEFAULT"]
                                           + ",最大字符长度:" + r["CHARACTER_MAXIMUM_LENGTH"].ToString()
                                           + ",数值精度:" + r["NUMERIC_PRECISION"].ToString()
                                           + ",数值精度基数:" + r["NUMERIC_PRECISION_RADIX"].ToString()
                                           + ")";

                    if (!tempColumns.Contains(temp))
                    {
                        l.Add(new ResultInfo("缺少的列", temp2, "缺"));
                    }

                    for (int i = 0; i < tempColumnSettings.Count; i++)
                    {
                        if (tempColumnSettings[i].Contains(temp))
                        {
                            if (!tempColumnSettings.Contains(temp2))
                            {
                                l.Add(new ResultInfo("变更的列", temp2, tempColumnSettings[i]));
                            }
                        }
                    }
                }
            }

            return l;
        }

        /// <summary>
        /// 查找所有缺少的主/外键约束
        /// </summary>
        /// <param name="masterConn">主库连接</param>
        /// <param name="devisonConn">分库连接</param>
        ///<returns>结果信息</returns>
        private List<ResultInfo> MissedKeyConstraint(SqlConnection masterConn, SqlConnection devisonConn)
        {
            List<ResultInfo> l = new List<ResultInfo>();

            List<String> masterTbConstraintNames = new List<String>();
            List<String> devisonTbConstraintNames = new List<String>();

            SqlDataReader sdr;
            SqlCommand command = new SqlCommand();
            command.CommandText = "select * from INFORMATION_SCHEMA.CONSTRAINT_TABLE_USAGE";

            command.Connection = masterConn;

            masterConn.Open();
            sdr = command.ExecuteReader();
            if (sdr.HasRows)
            {
                while (sdr.Read())
                {
                    masterTbConstraintNames.Add("[" + sdr["TABLE_NAME"] + "]." + "[" + sdr["CONSTRAINT_NAME"].ToString() + "]");
                }
            }
            masterConn.Close();

            command.Connection = devisonConn;
            devisonConn.Open();
            sdr = command.ExecuteReader();
            if (sdr.HasRows)
            {
                while (sdr.Read())
                {
                    devisonTbConstraintNames.Add("[" + sdr["TABLE_NAME"] + "]." + "[" + sdr["CONSTRAINT_NAME"].ToString() + "]");
                }
            }
            devisonConn.Close();

            foreach (string key in masterTbConstraintNames)
            {
                if (!devisonTbConstraintNames.Contains(key))
                {
                    l.Add(new ResultInfo("缺少的键约束", key, "缺"));
                }
            }

            return l;
        }

        #endregion

        //select * from INFORMATION_SCHEMA.CONSTRAINT_TABLE_USAGE -- 查询连接的库中所有定义了约束的所有表及其约束名
        //select * from INFORMATION_SCHEMA.KEY_COLUMN_USAGE --返回当前数据库中作为主键/外键约束的所有列。
        //select * from INFORMATION_SCHEMA.TABLES --查询所有表的信息
        //select * from INFORMATION_SCHEMA.COLUMNS --查询所有列的信息

        //COLUMN_NAME
        //TABLE_NAME
        //COLUMN_DEFAULT
        //IS_NULLABLE
        //DATA_TYPE
        //CHARACTER_MAXIMUM_LENGTH
        //NUMERIC_PRECISION
        //NUMERIC_PRECISION_RADIX

        //CONSTRAINT_NAME
    }

    /// <summary>
    /// 表示比较结果的信息结构
    /// </summary>
    internal struct ResultInfo
    {
        public string ItemName;
        public string MasterValue;
        public string DevisonValue;

        /// <summary>
        /// 构造比较结果信息结构
        /// </summary>
        /// <param name="itemName">比较项目名称</param>
        /// <param name="masterValue">主库值</param>
        /// <param name="devisonValue">分库值</param>
        public ResultInfo(string itemName, string masterValue, string devisonValue)
        {
            ItemName = itemName;
            MasterValue = masterValue;
            DevisonValue = devisonValue;
        }
    }
}

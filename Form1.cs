using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using System.IO;

namespace DBAnalyzer
{
    public partial class Form1 : Form
    {
        public Form1()
        {
            InitializeComponent();
        }

        //启动分析
        private void button_Analyze_Click(object sender, EventArgs e)
        {
            if (textBox1.Text.Length > 0 && textBox2.Text.Length > 0)
            {
                try
                {
                    Analyzer analyzer = new Analyzer(textBox1.Text, textBox2.Text);
                    dataGridView1.DataSource = analyzer.Analyze();
                }
                catch (Exception ex)
                {
                    MessageBox.Show("分析失败", ex.Message);
                }
            }
        }

        //导出报告
        private void contextMenuStrip1_ItemClicked(object sender, ToolStripItemClickedEventArgs e)
        {
            StringBuilder text = new StringBuilder();
            //是否有数据
            if (dataGridView1.RowCount > 0)
            {
                text.AppendLine("数据库对比分析报告 " + DateTime.Now.ToString("D"));
                text.AppendLine("/*******************************/");
                text.AppendLine("主数据库:");
                text.AppendLine(textBox1.Text);
                text.AppendLine("/*******************************/");
                text.AppendLine("分数据库:");
                text.AppendLine(textBox2.Text);
                text.AppendLine("/*******************************/");
                for (int i = 0; i < dataGridView1.RowCount; i++)
                {
                    string tempLine = String.Empty;
                    tempLine += (i+1).ToString() + ".\r\n";
                    tempLine += "项目:" + dataGridView1.Rows[i].Cells["ItemName"].Value.ToString() + "\r\n";
                    tempLine += "主数据库:\r\n" + dataGridView1.Rows[i].Cells["Master"].Value.ToString();
                    tempLine += "\r\n分数据库:\r\n" + dataGridView1.Rows[i].Cells["Division"].Value.ToString();
                    tempLine += "\r\n----\r\n";
                    text.AppendLine(tempLine);
                }

                //调出文件对话框保存结果
                saveFileDialog1.FileName = "数据库对比分析报告.txt";
                saveFileDialog1.Filter = "文本文件(*.txt)|*.txt|All files (*.*)|*.*";
                DialogResult dr = saveFileDialog1.ShowDialog(this);
                if (dr == System.Windows.Forms.DialogResult.OK || dr == System.Windows.Forms.DialogResult.Yes)
                {
                    string fn = saveFileDialog1.FileName;                    
                    StreamWriter sw = File.CreateText(fn);
                    sw.Write(text);
                    sw.Flush();
                    sw.Close();
                }
            }
            else
            {
                return;
            }
        }

    }
}

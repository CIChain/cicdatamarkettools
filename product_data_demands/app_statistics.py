import time
import smtplib
from email.mime.text import MIMEText
from email.header import Header
import os
import sys
path = os.getcwd()
sys.path.append(path)
import config
from mysqldb import Mysqldb

class AppStatistics():
    def __init__(self):
        self.db = Mysqldb(config.MySqlHostTicker, config.MySqlUserTicker, config.MySqlPasswdTicker,
                          config.MySqlDbTicker, config.MySqlPortTicker)
        
    def node_total_count(self):
        select_str = 'SELECT id from users'
        res_sel = self.db.select(select_str)
        return len(res_sel)
    
    #start_time,表示统计的开始时间
    #record_type:两种类型，stealCrystal：偷水晶,collectCrystal：收取水晶
    def collect_total_count(self, record_type, start_time):
        select_str = 'select *,count(uid) as count from balance_record WHERE record_type = "' + record_type +'" and txtime > ' + str(start_time) + ' group by uid having count>0'
        #select_str = 'SELECT uid FROM balance_record WHERE record_type = "collectCrystal" and txtime > ' + str(start_time)
        res_sel = self.db.select(select_str)
        return len(res_sel)
    
    def insurance_total_count(self):
        select_str = 'select b.name, count(*) as count from orders as a, products as b WHERE a.product_id = b.product_id group by a.product_id'
        res_sel = self.db.select(select_str)
        return res_sel
    
    def adv_child_node_count(self):
        select_str = 'select parent_id, count(*) as count from invite_agent group by parent_id'
        res_sel = self.db.select(select_str)
        total = 0
        for node in res_sel:
            total += node[1]
            
        return total / len(res_sel)
    
    def send_email(self, mail_text):
        smtp_server = 'smtp.exmail.qq.com'
        from_addr = 'mayaoguang@datebao.com'
        receivers = ['mayaoguang@datebao.com']
        password = 'Myg19891116'
        
        message = MIMEText(mail_text, 'plain', 'utf-8')
        message['From'] = Header("马耀光", 'utf-8')   # 发送者
        message['To'] =  Header("云保链", 'utf-8')        # 接收者
        subject = 'cichain用户数据统计'
        message['Subject'] = Header(subject, 'utf-8')
        
        server = smtplib.SMTP_SSL(smtp_server, 465)
        server.login(from_addr, password)
        try:
            server.sendmail(from_addr, receivers, message.as_string())
            server.quit()
            print("邮件发送成功")
        except smtplib.SMTPException:
            print("Error: 无法发送邮件")
    
if __name__ == "__main__":
    app_stat = AppStatistics()
    total_count = app_stat.node_total_count()
    mail_text = '节点总数：' + str(total_count)
    
    curr_time = int(time.time())
    daily_time = curr_time - 86400
    daily_collect_count = app_stat.collect_total_count('collectCrystal', daily_time)
    daily_steal_count = app_stat.collect_total_count('stealCrystal', daily_time)
    
    week_time = curr_time - 86400 * 7
    week_collect_count = app_stat.collect_total_count('collectCrystal', week_time)
    week_steal_count = app_stat.collect_total_count('stealCrystal', week_time)
    
    month_time = curr_time - 86400 * 30
    month_collect_count = app_stat.collect_total_count('collectCrystal', month_time)
    month_steal_count = app_stat.collect_total_count('stealCrystal', month_time)
    
    mail_text += '\n领取水晶用户数，单日：' + str(daily_collect_count) + ',7天：' + str(
            week_collect_count) + ',30天' + str(month_collect_count)
    
    mail_text += '\n偷取别人水晶用户数，单日：' + str(daily_collect_count) + ',7天：' + str(
            week_collect_count) + '30天：' + str(month_collect_count)
    
    insurance_counts = app_stat.insurance_total_count()
    mail_text += '\n险种出售总数：' + str(insurance_counts)
    adv_count = app_stat.adv_child_node_count()
    mail_text += '\n每个用户平均邀请好友数：' + str(adv_count)
    app_stat.send_email(mail_text)
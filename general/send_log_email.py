import sys
import os
import smtplib

sys.path.append("..")
from utils.db import db_config
from email.message import EmailMessage
from datetime import datetime

from dotenv.main import load_dotenv

load_dotenv()
# TO_MAIL_ARRAY = ["yash.malani@softqubes.com", "hardik.kanak@softqubes.com"]
TO_MAIL_ARRAY = ["hardik.kanak@softqubes.com"]


def send_Email_Report():
    try:
        today_date = datetime.today().date()
        tab1Header = ""
        tab1Values = ""
        values = []
        keys = []
        today_date = f"'{today_date}'"
        conn = db_config.get_db_connection()
        query = f'SELECT * FROM public."tbl_pullDate" where "pulledDate"={today_date};'

        result = conn.execute(query)
        myRes = result.fetchall()
        conn.close()
        if len(myRes) > 0:
            for i in myRes:
                keys.append(i.keys())
                values.append(list(i))
            tab1Header += f"""<tr>"""
            for key in keys[0]:
                tab1Header += f"""<th>{key}</th>"""
            tab1Header += f"""</tr>"""
            for value in values:
                tab1Values += f"""<tr>"""
                for k in value:
                    if k:
                        tab1Values += f"""<td>{k}</td>"""
                    else:
                        tab1Values += f"""<td></td>"""
                tab1Values += f"""</tr>"""

            msg = EmailMessage()
            msg['Subject'] = f"Today's Script Report({today_date}) "
            msg['From'] = os.environ['SMTP_EMAIL']
            msg['To'] = TO_MAIL_ARRAY
            msg.set_content('table1')
            html = """
                <!DOCTYPE html>
                    <html>
                        <head>
                            <style>
                            table {
                            font-family: arial, sans-serif;
                            border-collapse: collapse;
                            width: 100%;
                            }

                            td, th {
                            border: 1px solid #dddddd;
                            text-align: left;
                            padding: 8px;
                            }

                            tr:nth-child(even) {
                            background-color: #dddddd;
                            }
                            </style>
                        </head>
                        <body>
                            <h2>Atica Scheduled Result</h2>
                            <table>
                            """ + tab1Header + """
                            """ + tab1Values + """

                            </table>
                        </body>
                    </html>
                    """
            msg.add_alternative(html, subtype='html')
            with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
                smtp.login(os.environ['SMTP_EMAIL'], os.environ['SMTP_PASSWORD'])
                smtp.send_message(msg)
                print("email sent successfully")

    except Exception as e:
        print(e)


if __name__ == '__main__':
    send_Email_Report()

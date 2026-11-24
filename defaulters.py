import apache_beam as beam
import os
from datetime import datetime

p = beam.Pipeline()

def calculate_points(element):
    Customer_id,First_name,Last_name,Relationship_no,card_type,max_limit,spent,Cash_withdrawn ,payemrnt_cleared, Last_date = element.split(',')
    spent = int(spent)
    payment_cleared = int(payemrnt_cleared)
    max_limit = int(max_limit)
    
    key_name = Customer_id + ', ' + First_name + ' ' + Last_name
    defaulter_point = 0

    if payment_cleared < (spent * 0.7):
        defaulter_point += 1

    if (spent == max_limit) and (payment_cleared < spent):
        defaulter_point += 1

    if (spent == max_limit) and (payment_cleared < (spent * 0.7)):
        defaulter_point += 1

    return key_name, defaulter_point

def format_result(sum_pair):
    key_name, points = sum_pair

    return str(key_name) +', ' + str(points) + ' fraud_points'

def calculate_late_payments(elements):
    due_date = datetime.strptime(elements[6].rstrip().lstrip(), '%Y-%m-%d')
    payment_date = datetime.strptime(elements[8].rstrip().lstrip(), '%Y-%m-%d')

    if payment_date <= due_date:
        elements.append('0')
    else:
        elements.append('1')

    return elements

def format_output(sum_pair):
    key_name, miss_month = sum_pair

    return str(key_name) +', ' + str(miss_month) + ' missed'

def calculate_month(input_list):
    payment_date = datetime.strptime(input_list[8].rstrip().lstrip(), '%Y-%m-%d')
    input_list.append(str(payment_date.month))

    return input_list

def calcuate_personal_loan_defaulter(input):
    max_allowed_missed_months = 4
    max_alloweed_consecutive_missed_months = 2

    name, months_list = input
    months_list.sort()
    sorted_months = months_list
    total_payments = len(sorted_months)
    missed_payments = 12 - total_payments

    if missed_payments > max_allowed_missed_months:
        return name, missed_payments

    consecutive_missed_months = 0
    temp = sorted_months[0] - 1
    if temp > consecutive_missed_months:
        consecutive_missed_months = temp

    temp = 12 - sorted_months[total_payments-1]
    if temp > consecutive_missed_months:
        consecutive_missed_months = temp

    for i in range(1, len(sorted_months)):
        temp = sorted_months[i] - sorted_months[i-1] - 1
        if temp > consecutive_missed_months:
            consecutive_missed_months = temp
    if consecutive_missed_months > max_alloweed_consecutive_missed_months:

        return name, consecutive_missed_months
    
    return name, 0

def return_tuple(element):
    thisTuple = element.split(', ')

    return (thisTuple[0], thisTuple[1:])

def format_both_result(grouped):
    customer_id, results = grouped
    card_results = [' '.join(parts) for parts in results['card_defaulters']]
    loan_results = [' '.join(parts) for parts in results['loan_defaulters']]

    return customer_id + ': card: ' + '; '.join(card_results) + ' | loan: ' + '; '.join(loan_results)

os.makedirs('output', exist_ok=True)

card_defaulter = (
    p
    |"Read credit card data" >> beam.io.ReadFromText('cards.csv', skip_header_lines=1)
    |"Calculate defaulter point" >> beam.Map(calculate_points)
    |"Combine defaulter points" >> beam.CombinePerKey(sum)
    |"Filter card defaulters" >> beam.Filter(lambda x: x[1] > 0)
    |"Format output" >> beam.Map(format_result)
)
card_defaulter |"Write credit card data" >> beam.io.WriteToText('output/card_skippers')
card_defaulters_by_customer = card_defaulter |"Key card defaulters" >> beam.Map(return_tuple)

personal_loan_defaulter = (
    p
    |"Read personal loan data" >> beam.io.ReadFromText('loan.csv', skip_header_lines=1)
    |"Split" >> beam.Map(lambda row: row.split(','))
    |"Filter Personal Loan" >> beam.Filter(lambda element: (element[5]).rstrip().lstrip() == 'Personal Loan')
    |"Split and append new month column" >> beam.Map(calculate_month)
    |"Make key value pair loan" >> beam.Map(lambda element: (element[0] + ', ' + element[1] + ' ' + element[2], int(element[9])))
    |"Group personal loan based on month" >> beam.GroupByKey()
    |"Check for personal loan defaulters" >> beam.Map(calcuate_personal_loan_defaulter)
    |"Filter only personal loan defaulters" >> beam.Filter(lambda element: element[1] > 0)
    |"Format personal loan output" >> beam.Map(format_output)
)

medical_loan_defaulter = (
    p
    |"Read medical loan data" >> beam.io.ReadFromText('loan.csv', skip_header_lines=1)
    |"Split Rows" >> beam.Map(lambda x: x.split(','))
    |"Filter Medical Loan" >> beam.Filter(lambda element: (element[5]).rstrip().lstrip() == 'Medical Loan')
    |"Calculate late payments" >> beam.Map(calculate_late_payments)
    |"Make key value pair" >> beam.Map(lambda element: (element[0] + ', ' + element[1] + ' ' + element[2], int(element[9])))
    |"Group medical loan based on month" >> beam.CombinePerKey(sum)
    |"Check for medical loan defaulters" >> beam.Filter(lambda element: element[1] >= 3)
    |"format medical loan output" >> beam.Map(format_output)
)

final_loan_defaulters = (
    (personal_loan_defaulter, medical_loan_defaulter)
    |"Merge all defaulters" >> beam.Flatten()
)
final_loan_defaulters |"Write all defaulters to the text file" >> beam.io.WriteToText('output/loan_defaulters')
loan_defaulters_by_customer = final_loan_defaulters |"Key loan defaulters" >> beam.Map(return_tuple)

both_defaulters = (
    {'card_defaulters': card_defaulters_by_customer, 'loan_defaulters': loan_defaulters_by_customer}
    |beam.CoGroupByKey()
    |"Keep customers flagged by both" >> beam.Filter(
        lambda element: element[1]['card_defaulters'] and element[1]['loan_defaulters']
    )
    |"Format both defaulters" >> beam.Map(format_both_result)
    |"Write p3 results" >> beam.io.WriteToText('output/both')
)

p.run().wait_until_finish()
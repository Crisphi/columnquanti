
# -*- coding: utf-8 -*-
"""
Created on Tue Dec 03 11:34:11 2021

@author: Cristian Ortega Singer
"""

import csv
import os
from collections import OrderedDict

os.chdir("./metadata") #replace with path to directory where the data sets to be processed are stored
path = os.getcwd()
files = []
files = os.listdir(path)
columnsNone = {} #dict with all exceptions; can be exported if needed; right now the script doesn't export it
columnsTotal = {}
columnsNoneAgg = {}
columnsTotalAgg = {}
columnsCSVExport = {}
header = []

for file in files:
    print(file)
    columnsNoneFile = {}
    columnsTotalFile = {}
    with open(file, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile, delimiter=",")
        for row in reader:
            for key in row:
                if key:
                    if key in columnsTotalFile:
                        columnsTotalFile[key] = columnsTotalFile[key] + 1
                    else:
                        columnsTotalFile[key] = 1
                    if key not in columnsNoneFile:
                        columnsNoneFile[key] = 0
                    if row[key] != "None" and row[key] != "":
                        columnsNoneFile[key] = columnsNoneFile[key] + 1

    columnsNone[file] = columnsNoneFile.copy()
    columnsTotal[file] = columnsTotalFile.copy()



for f in columnsNone:
    for column in columnsNone[f]:
        if column in columnsNoneAgg:
            columnsNoneAgg[column] = columnsNoneAgg[column] + columnsNone[f][column]
        else:
            columnsNoneAgg[column] = columnsNone[f][column]

for f in columnsTotal:
    for column in columnsTotal[f]:
        if column in columnsTotalAgg:
            columnsTotalAgg[column] = columnsTotalAgg[column] + columnsTotal[f][column]
        else:
            columnsTotalAgg[column] = columnsTotal[f][column]

for cN in columnsNoneAgg:
    print(cN, columnsNoneAgg[cN])
    cnameN = cN + "_Entry"
    columnsCSVExport[cnameN] = columnsNoneAgg[cN]
    cnameT = cN + "_Total"
    columnsCSVExport[cnameT] = columnsTotalAgg[cN]
    header.append(cnameN)
    header.append(cnameT)

os.chdir("../done")
with open("quantifiedcolumns.csv", "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames = header)

        writer.writeheader()

        writer.writerow(columnsCSVExport)

with open("quantifiedcolumns.txt", "w", encoding="utf-8") as txtfile:
    for cN in columnsNoneAgg:
        txtfile.write(cN + ":-----\n" + str(columnsNoneAgg[cN]) + " Entries of " + str(columnsTotalAgg[cN]) + "\n\n")

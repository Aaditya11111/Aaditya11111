import glob
import parsepdf
pdffiles = []
datapath="testdata/*.pdf"
for file in glob.glob(datapath):
     pdffiles.append(file)
print(pdffiles)
for f in pdffiles:
    parsepdf.parseAndSave(f,"output/"+f.split("/")[1]+".csv")
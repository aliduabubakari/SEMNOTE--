import semtui

semtui.SEMTUI_URI = "http://localhost:3003/api/"

#print(semtui.addDataset(r'C:\Users\user\Documents\BICOCCA\archive (3).zip', 'Adventure_Dataset', inputType='file'))

#print(semtui.addDataset("C:\\Users\\user\\Documents\\BICOCCA\\archive (3).zip", 'Adventure_Dataset', inputType='file'))

datasets = semtui.getDatasetsList()
print(f"Available datasets on {semtui.SEMTUI_URI}:")
for dataset in datasets["data"]:
    print(dataset)

# Get the metadata of a specific dataset

print(datasets)
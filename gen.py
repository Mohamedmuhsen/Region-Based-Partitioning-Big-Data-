import random
 
regions = ['North', 'South', 'East', 'West']
output_file = 'C:\\Users\\El-Ashry\\OneDrive\\Desktop\\bigdatapr\\stores_1gb.csv' 
print("Generating 1GB dataset...")
 
with open(output_file, 'w') as f:
    f.write("store_id,region,visitors,daily_revenue,opening_date\n")
    for i in range(1, 16000001):
        region   = random.choice(regions)
        visitors = random.randint(100, 5000)
        revenue  = visitors * random.randint(4, 8)
        year     = random.randint(2018, 2023)
        month    = random.randint(1, 12)
        day      = random.randint(1, 28)
        f.write(f"S{i:08d},{region},{visitors},{revenue},{year}-{month:02d}-{day:02d}\n")
        if i % 2000000 == 0:
            print(f"  {i:,} records written...")
 
print("Done! File saved as stores_1gb.csv")
 
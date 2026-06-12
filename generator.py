import csv, os, random, time
from datetime import datetime, timedelta

INPUT_DIR = "data/input_stream"
os.makedirs(INPUT_DIR, exist_ok=True)

i = 0
while True:
    i += 1
    tmp = os.path.join(INPUT_DIR, f"_tmp_{i}.csv")
    final = os.path.join(INPUT_DIR, f"events_{i}_{int(time.time())}.csv")
    with open(tmp, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["event_time", "user_id", "category", "amount", "status"])
        for _ in range(random.randint(3, 8)):
            r = random.random()
            if r < 0.2:
                delay = 90
            elif r < 0.3:
                delay = 600
            else:
                delay = 0
            et = datetime.now() - timedelta(seconds=delay)
            w.writerow([
                et.strftime("%Y-%m-%d %H:%M:%S"),
                f"u{random.randint(1, 50):03d}",
                random.choice(["books", "electronics", "clothes", "food", "toys"]),
                round(random.uniform(5, 300), 2),
                random.choice(["paid", "pending", "cancelled"]),
            ])
    os.rename(tmp, final)
    print("Dodano:", final)
    time.sleep(5)

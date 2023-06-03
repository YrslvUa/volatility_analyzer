import csv
import os
import time
from threading import Thread


# from google_drive_downloader import GoogleDriveDownloader as gdd

# gdd.download_file_from_google_drive(file_id='1l5sia-9c-t91iIPiGyBc1s9mQ8RgTNqb',
#                                     dest_path='./mnist.zip',
#                                     unzip=True)
def time_track(func):
    def surrogate(*args, **kwargs):
        started_at = time.time()
        result = func(*args, **kwargs)
        ended_at = time.time()
        elapsed = round(ended_at - started_at, 4)
        print(f'Функция работала {elapsed} секунд(ы)')
        return result
    return surrogate


class TickerHandler(Thread):

    def __init__(self, folder):
        super().__init__()
        self.folder = folder
        self.volatility_data = {}

    def run(self):
        for file in os.listdir(self.folder):
            file_path = os.path.join(self.folder, file)
            with open(file_path, 'r') as csvfile:
                reader = csv.reader(csvfile)
                next(reader)
                min_price = float('inf')
                max_price = float('-inf')
                ticker = None

                for row in reader:
                    ticker = row[0]
                    price = float(row[2])
                    min_price = min(min_price, price)
                    max_price = max(max_price, price)

                average_price = (max_price + min_price) / 2
                volatility = ((max_price - min_price) / average_price) * 100
                self.volatility_data[ticker] = volatility



@time_track
def main():
    folder = 'trades'
    threads = []

    for _ in range(4):
        handler = TickerHandler(folder=folder)
        threads.append(handler)
        handler.start()

    for thread in threads:
        thread.join()

    volatility_data = {}
    for thread in threads:
        volatility_data.update(thread.volatility_data)

    sorted_data = sorted(volatility_data.items(), key=lambda x: x[1], reverse=True)
    max_volatility = sorted_data[:3]
    min_volatility = sorted_data[-3:]
    zero_volatility = [item for item in sorted_data if item[1] == 0]

    print("Max volatility:")
    for ticker, volatility in max_volatility:
        print(f"{ticker} - {volatility:.2f} %")

    print("\nMin volatility:")
    for ticker, volatility in min_volatility:
        print(f"{ticker} - {volatility:.2f} %")

    print("\nZero volatility:")
    zero_tickers = sorted([ticker for ticker, _ in zero_volatility])
    print(", ".join(zero_tickers))


if __name__ == "__main__":
    main()

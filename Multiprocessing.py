import os
import csv
from multiprocessing import Process, Manager

from utils import time_track


class TickerHandler(Process):

    def __init__(self, folder, volatility_data):
        super().__init__()
        self.folder = folder
        self.volatility_data = volatility_data

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
    processes = []

    with Manager() as manager:
        volatility_data = manager.dict()

        for _ in range(4):
            handler = TickerHandler(folder=folder, volatility_data=volatility_data)
            processes.append(handler)
            handler.start()

        for process in processes:
            process.join()

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

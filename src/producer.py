from faker import Faker

fake = Faker("pt_BR")

regions = ["Sul", "Sudeste", "Centro-Oeste", "Norte", "Nordeste"]
vendors = ["Kaio Silva", "Luciano Galvão", "Fabio Melo", "Estagiario"]


def generate_fake_order(start_date="-30d", end_date="today"):
    quantity = fake.random_int(min=1, max=10)
    unit_price = fake.random_int(min=50, max=200)
    total_price = quantity * unit_price
    return {
        "order_id": fake.uuid4(),
        "order_date": fake.date_between(start_date=start_date, end_date=end_date),
        "product_id": fake.uuid4(),
        "region": fake.random_element(elements=regions),
        "vendor": fake.random_element(elements=vendors),
        "quantity": quantity,
        "unit_price": unit_price,
        "total_price": total_price,
    }


def generate_fake_orders_csv(n_rows=1000):
    import pandas as pd

    data = [generate_fake_order() for _ in range(n_rows)]
    pd.DataFrame(data).to_csv("fake_orders.csv", index=False)


if __name__ == "__main__":
    generate_fake_orders_csv()

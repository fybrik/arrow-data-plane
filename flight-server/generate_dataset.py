#!/usr/bin/python3
import random
import names
from faker import Faker

NUM_ROWS = 1000
SEED=10

fake = Faker()
random.seed(SEED)
Faker.seed(SEED)
for i in range(NUM_ROWS):
    name=names.get_full_name()
    age=random.randrange(15, 25)
    building_number=fake.building_number()
    street=fake.street_name() + " " + fake.street_suffix()
    city=fake.city()
    country=fake.country()
    postcode=fake.postcode()
    print(name+","+str(age)+","+building_number+","+street+","+city+","+country+","+postcode)

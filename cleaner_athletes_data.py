import re
import pandas as pd

AGE_MAX = 125
AGE_MIN = 15
HEIGHT_MAX = 300
HEIGHT_MIN = 50
WEIGHT_MAX = 700
WEIGHT_MIN = 20


def convert_mass_from_imperial_to_metric(mass: str) -> float:
    kg = 0
    if mass.endswith("lb"):
        kg = float(mass[:-3]) * 0.453592
    elif mass.endswith("kg"):
        kg = float(mass[:-3])
    return kg


def convert_distance_from_imperial_to_metric(distance: str) -> float:
    cm = 0
    if re.match(r'\d+\'\d+\"', distance):
        feet, inches = distance[:-1].split('\'')
        cm = float(feet) * 30.48 + float(inches) * 2.54
    elif distance.endswith("cm"):
        cm = float(distance[:-3])
    return cm


def convert_time_to_seconds(time: str) -> float:
    seconds = 0
    if re.match(r'\d+:\d+', time):
        minutes, seconds = time.split(':')
        seconds = int(minutes) * 60 + int(seconds)
    elif re.match(r'\d+:\d+:\d+', time):
        hours, minutes, seconds = time.split(':')
        seconds = int(hours) * 3600 + int(minutes) * 60 + int(seconds)
    return seconds


def process_row(row):
    """
    Processes a row of data from the scraped data.
    """
    try:
        # replace "Korea, Republic of" with "Republic of Korea"
        row = row.replace("Korea, Republic of", "Republic of Korea")
        # Remove any leading or trailing whitespace
        row = row.strip()
        # Count the number of commas in the row if it is greater than 3 then raise an exception
        if row.count(',') > 3:
            raise Exception("Too many commas in row")
        id, name, country_row, *_ = row.split(',')
        # country
        regexp_country = re.compile(r'(.*) CFID (.*)')
        country_result = re.search(regexp_country, country_row)
        if country_result:
            country = country_result.group(1)
        else:
            country = "--"
        # division
        regexp_division = re.compile(r'(.*) Division (.*) Age (.*)')
        division_result = re.search(regexp_division, row)
        if division_result:
            division = division_result.group(2)
        else:
            division = "--"
        # age
        regexp_age = re.compile(r'(.*) Age (.*) Height (.*)')
        age_result = re.search(regexp_age, row)
        if age_result:
            age = age_result.group(2)
        else:
            age = "--"
        # height
        regexp_height = re.compile(r'(.*) Height (.*) Weight (.*)')
        height_result = re.search(regexp_height, row)
        if height_result:
            height = height_result.group(2)
        else:
            height = "--"
        # weight
        regexp_weight = re.compile(r'(.*) Weight (.*) Affiliate (.*)')
        weight_result = re.search(regexp_weight, row)
        if weight_result:
            weight = weight_result.group(2)
        else:
            weight = "--"
        # affiliate
        regexp_affiliate = re.compile(r'(.*) Affiliate (.*),(.*)')
        affiliate_result = re.search(regexp_affiliate, row)
        if affiliate_result:
            affiliate = affiliate_result.group(2)
        else:
            affiliate = "--"
        # benchmark stats
        regexp_benchmark_stats = re.compile(
            r'(.*)Back Squat (.*) Clean and Jerk (.*) Snatch (.*) Deadlift (.*) Fight Gone Bad (.*) Max Pull-ups (.*) Fran (.*) Grace (.*) Helen (.*) Filthy 50 (.*) Sprint 400m (.*) Run 5k (.*)')
        benchmark_stats_result = re.search(regexp_benchmark_stats, row)
        back_squat = benchmark_stats_result.group(2)
        clean_and_jerk = benchmark_stats_result.group(3)
        snatch = benchmark_stats_result.group(4)
        deadlift = benchmark_stats_result.group(5)
        fight_gone_bad = benchmark_stats_result.group(6)
        max_pull_ups = benchmark_stats_result.group(7)
        
        fran = convert_time_to_seconds(benchmark_stats_result.group(8))
        grace = convert_time_to_seconds(benchmark_stats_result.group(9))
        helen = convert_time_to_seconds(benchmark_stats_result.group(10))
        filthy_50 = convert_time_to_seconds(benchmark_stats_result.group(11))
        sprint_400m = convert_time_to_seconds(benchmark_stats_result.group(12))
        run_5k = convert_time_to_seconds(benchmark_stats_result.group(13))

        height_cm = convert_distance_from_imperial_to_metric(height)
        weight_kg = convert_mass_from_imperial_to_metric(weight)

        back_squat_kg = convert_mass_from_imperial_to_metric(back_squat)
        clean_and_jerk_kg = convert_mass_from_imperial_to_metric(
            clean_and_jerk)
        snatch_kg = convert_mass_from_imperial_to_metric(snatch)
        deadlift_kg = convert_mass_from_imperial_to_metric(deadlift)

        return {
            'id': id,
            'name': name,
            'country': country,
            'division': division,
            'gender': "Female" if "Women" in division else ("Male" if "Men" in division else "--"),
            'age': age if AGE_MIN <= int(age) <= AGE_MAX else "--",
            'height': height_cm if HEIGHT_MIN <= int(height_cm) <= HEIGHT_MAX else "--",
            'weight': weight_kg if WEIGHT_MIN <= int(weight_kg) <= WEIGHT_MAX else "--",
            'IMC': (weight_kg / (height_cm/100)**2) if height_cm > 0 else 0,
            'affiliate': affiliate,
            'back_squat': back_squat_kg,
            'clean_and_jerk': clean_and_jerk_kg,
            'snatch': snatch_kg,
            'deadlift': deadlift_kg,
            'fight_gone_bad': fight_gone_bad,
            'max_pull_ups': max_pull_ups,
            'fran': fran,
            'grace': grace,
            'helen': helen,
            'filthy_50': filthy_50,
            'sprint_400m': sprint_400m,
            'run_5k': run_5k
        }
    except Exception as e:
        print(f"Error processing row: {row}")
        print(e)
        with open('processed/athlete_data_error.csv', 'a') as f:
            f.write(f'{row},{e}\n')


def process_file(file):
    data = []
    with open(file, 'r') as f:
        for line in f:
            if processed_line := process_row(line):
                data.append(processed_line)
    return data


pd.DataFrame(process_file('athletes.csv')
             ).to_csv('processed/data.csv')

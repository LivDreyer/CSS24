import os
from joblib import Parallel, delayed
from tqdm import tqdm
import pandas as pd
import requests
import time


ic2s2_papers_df = pd.read_csv('IC2S2_papers_dataset.csv')


co_author_ids = set()
for ids_str in ic2s2_papers_df['author_ids']:
    ids_list = ids_str.split(';')
    co_author_ids.update(ids_list)


ic2s2_authors_df = pd.read_csv('IC2S2_authors_dataset.csv')
ic2s2_author_ids = set(ic2s2_authors_df['id'].tolist())

co_author_ids = co_author_ids.difference(ic2s2_author_ids)


social_sciences = {"Sociology", "Psychology", "Economics", "Political Science"}
quantitative_disciplines = {"Mathematics", "Physics", "Computer Science"}

def meets_criteria(work_detail):
    concepts = {concept['display_name'] for concept in work_detail.get('concepts', [])}
    social_science_present = any(concept in social_sciences for concept in concepts)
    quantitative_present = any(concept in quantitative_disciplines for concept in concepts)
    return social_science_present and quantitative_present

if not os.path.isfile('IC2S2_co_authors_dataset.csv'):  
    print("Co-author dataset does not exist. Fetching co-author details...")


    def fetch_co_author_details_worker(author_id):
        url = f"https://api.openalex.org/authors/{author_id}"
        response = requests.get(url)
        if response.status_code == 429:
          
            time.sleep(0.1)
            return fetch_co_author_details_worker(author_id)
        elif response.status_code != 200:
            print(f"Error fetching details for author {author_id}: {response.status_code}")
            return None

        data = response.json()
        return {
            'id': data['id'],
            'display_name': data['display_name'],
            'works_api_url': data['works_api_url'],
            'h_index': data.get('h_index', None),
            'works_count': data.get('works_count', None),
            'country_code': data['last_known_institution']['country_code'] if data.get('last_known_institution') else None
        }

    co_authors_details = Parallel(n_jobs=5)(delayed(fetch_co_author_details_worker)(author_id) for author_id in tqdm(co_author_ids, desc="Fetching co-authors details"))

    co_authors_details = [details for details in co_authors_details if details is not None and 5 <= details['works_count'] <= 5000]


    co_authors_df = pd.DataFrame(co_authors_details)


    co_authors_df.to_csv('IC2S2_co_authors_dataset.csv', index=False)
else:
    print("Co-author dataset already exists. Skipping co-author info gathering.")


ic2s2_co_authors_df = pd.read_csv('IC2S2_co_authors_dataset.csv')


def fetch_works_and_abstracts_for_co_author_worker(author_id):
    works_details = []
    abstracts_details = []
    
    url = f"https://api.openalex.org/works?filter=author.id:{author_id},cited_by_count:>10&per-page=200"
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Error fetching works for co-author {author_id}: {response.status_code}")
        return [], []  

    data = response.json()
    if 'results' in data:
        for work in data['results']:
            author_ids = [authorship['author']['id'] for authorship in work.get('authorships', [])]
            if len(author_ids) < 10:
                if meets_criteria(work):  
                    works_details.append({
                        'id': work['id'],
                        'publication_year': work.get('publication_year'),
                        'cited_by_count': work.get('cited_by_count'),
                        'title': work.get('title'),
                        'abstract_inverted_index': work.get('abstract_inverted_index', {}),
                        'author_ids': author_ids
                    })
                    abstracts_details.append({
                        'id': work['id'],
                        'abstract': work.get('abstract'),
                    })

    return works_details, abstracts_details

total_co_authors = len(ic2s2_co_authors_df)
with tqdm(total=total_co_authors, desc="Fetching co-authors works") as pbar:

    results = Parallel(n_jobs=5)(delayed(fetch_works_and_abstracts_for_co_author_worker)(author_id) for author_id in tqdm(ic2s2_co_authors_df['id'], desc="Fetching co-authors works"))
    pbar.update(total_co_authors)  


co_authors_works_data, co_authors_abstracts_data = zip(*results)


co_authors_works_df = pd.concat([pd.Series(data) for sublist in co_authors_works_data for data in sublist], axis=1).T

co_authors_abstracts_df = pd.concat([pd.Series(data) for sublist in co_authors_abstracts_data for data in sublist], axis=1).T


ic2s2_abstracts_df = pd.read_csv('IC2S2_abstracts_dataset.csv')

final_authors_df = pd.concat([ic2s2_authors_df, ic2s2_co_authors_df]).drop_duplicates(subset='id')

final_papers_df = pd.concat([ic2s2_papers_df, co_authors_works_df]).drop_duplicates(subset='id')

final_abstracts_df = pd.concat([ic2s2_abstracts_df, co_authors_abstracts_df]).drop_duplicates(subset='id')

final_authors_df.to_csv('final_authors_dataset.csv', index=False)
final_papers_df.to_csv('final_papers_dataset.csv', index=False)

final_abstracts_df.to_csv('final_abstracts_dataset.csv', index=False)

print("Exercise 2 completed successfully.")

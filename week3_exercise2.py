from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import requests


ic2s2_papers_df = pd.read_csv('IC2S2_papers_dataset.csv')
co_author_ids = set()
ic2s2_abstracts_df = pd.read_csv('IC2S2_abstracts_dataset.csv')
for ids_str in ic2s2_papers_df['author_ids']:
    ids_list = eval(ids_str)  
    co_author_ids.update(ids_list)


ic2s2_authors_df = pd.read_csv('researchers_data.csv')
ic2s2_author_ids = set(ic2s2_authors_df['id'].tolist())


co_author_ids = co_author_ids.difference(ic2s2_author_ids)


def fetch_co_author_details(author_id):
    url = f"https://api.openalex.org/authors/{author_id}"
    response = requests.get(url)
    if response.status_code != 200:
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

def parallel_fetch_co_authors(author_ids):
    co_authors_details = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_author = {executor.submit(fetch_co_author_details, author_id): author_id for author_id in author_ids}
        for future in as_completed(future_to_author):
            details = future.result()
            if details is not None:
                co_authors_details.append(details)
    return co_authors_details

co_authors_details = parallel_fetch_co_authors(co_author_ids)
co_authors_df = pd.DataFrame(co_authors_details)
co_authors_df.to_csv('IC2S2_co_authors_dataset.csv', index=False)


def fetch_works_for_co_author(author_id):
    works_details = []
    page = 1
    social_sciences = {"Sociology", "Psychology", "Economics", "Political Science"}
    quantitative_disciplines = {"Mathematics", "Physics", "Computer Science"}

    while True:
        url = f"https://api.openalex.org/works?filter=author.id:{author_id},cited_by_count:>10&per-page=200&page={page}"
        response = requests.get(url)
        if response.status_code != 200:
            print(f"Error fetching works for co-author {author_id}: {response.status_code}")
            break

        data = response.json()
        if 'results' not in data or not data['results']:
            break

        for work in data['results']:
            author_ids = [authorship['author']['id'] for authorship in work.get('authorships', [])]
            if len(author_ids) < 10:
                concepts = {concept['display_name'] for concept in work.get('concepts', [])}
                if concepts.intersection(social_sciences) and concepts.intersection(quantitative_disciplines):
                    works_details.append({
                        'id': work['id'],
                        'publication_year': work.get('publication_year'),
                        'cited_by_count': work.get('cited_by_count'),
                        'title': work.get('title'),
                        'abstract_inverted_index': work.get('abstract_inverted_index', {}),
                        'author_ids': author_ids
                    })

        page += 1
    return works_details


def parallel_fetch_works(author_ids):
    papers_data = []
    abstracts_data = []

    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_author = {executor.submit(fetch_works_for_co_author, author_id): author_id for author_id in author_ids}

        for future in as_completed(future_to_author):
            works_details = future.result()
            for work_detail in works_details:
                papers_data.append({
                    'id': work_detail['id'],
                    'publication_year': work_detail['publication_year'],
                    'cited_by_count': work_detail['cited_by_count'],
                    'author_ids': work_detail['author_ids']
                })
                if 'abstract_inverted_index' in work_detail:
                    abstracts_data.append({
                        'id': work_detail['id'],
                        'title': work_detail['title'],
                        'abstract_inverted_index': work_detail['abstract_inverted_index']
                    })

    return papers_data, abstracts_data


co_authors_works_data, co_authors_abstracts_data = parallel_fetch_works(list(co_author_ids))
co_authors_works_df = pd.DataFrame(co_authors_works_data)
co_authors_abstracts_df = pd.DataFrame(co_authors_abstracts_data)


final_authors_df = pd.concat([ic2s2_authors_df, co_authors_df]).drop_duplicates(subset='id')
final_authors_df.to_csv('final_authors_dataset.csv', index=False)

final_papers_df = pd.concat([ic2s2_papers_df, co_authors_works_df]).drop_duplicates(subset='id')
final_papers_df.to_csv('final_papers_dataset.csv', index=False)

final_abstracts_df = pd.concat([ic2s2_abstracts_df, co_authors_abstracts_df]).drop_duplicates(subset='id')
final_abstracts_df.to_csv('final_abstracts_dataset.csv', index=False)

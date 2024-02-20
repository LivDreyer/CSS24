from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import requests


file_path = "/Users/felixfrandsen/Computational_social_science/researchers_data.csv"
researchers_df = pd.read_csv(file_path)


author_ids = researchers_df['id'].apply(lambda x: x.split('/')[-1]).tolist()  

def fetch_works_for_author(author_id):
    works_details = []
    page = 1
    social_sciences = {"Sociology", "Psychology", "Economics", "Political Science"}
    quantitative_disciplines = {"Mathematics", "Physics", "Computer Science"}

    while True:
        url = f"https://api.openalex.org/works?filter=author.id:{author_id}&per-page=200&page={page}"
        response = requests.get(url)
        if response.status_code != 200:
            print(f"Error fetching works for author {author_id}: {response.status_code}")
            break

        data = response.json()
        if 'results' not in data or not data['results']:
            break

        for work in data['results']:
            concepts = {concept['display_name'] for concept in work.get('concepts', [])}
            if concepts.intersection(social_sciences) and concepts.intersection(quantitative_disciplines):
                # If a work is related to at least one social science AND one quantitative discipline
                works_details.append({
                    'id': work['id'],
                    'publication_year': work.get('publication_year'),
                    'cited_by_count': work.get('cited_by_count'),
                    'title': work.get('title'),
                    'abstract_inverted_index': work.get('abstract_inverted_index'),
                    'author_ids': [authorship['author']['id'] for authorship in work.get('authorships', [])]
                })

        page += 1
    return works_details

def parallel_fetch_works(author_ids):
    papers_data = []
    abstracts_data = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_author = {executor.submit(fetch_works_for_author, author_id): author_id for author_id in author_ids}
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



papers_data, abstracts_data = parallel_fetch_works(author_ids)


papers_df = pd.DataFrame(papers_data)
abstracts_df = pd.DataFrame(abstracts_data)


papers_df.to_csv("/Users/felixfrandsen/Computational_social_science/IC2S2_papers_dataset.csv", index=False)
abstracts_df.to_csv("/Users/felixfrandsen/Computational_social_science/IC2S2_abstracts_dataset.csv", index=False)

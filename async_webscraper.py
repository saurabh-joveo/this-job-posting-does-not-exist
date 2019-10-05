import aiohttp
import asyncio
import async_timeout
from bs4 import BeautifulSoup
import os
import sys
import time
from urllib.parse import urlparse


async def fetch(session, url):
    # Here I give a generous timeout
    # because I want as much data as I can get
    async with async_timeout.timeout(3600):
        async with session.get(url) as response:
            return await response.text()


async def get_soup(url):
    return BeautifulSoup(url, 'lxml')


async def extract_job_links(html):
    soup = await get_soup(html)
    job_links = [homepage + job['href']
                 for job in soup.findAll('a', {'target': '_blank'})
                 if len(job['href']) > 8 and job['href'][:7] == '/rc/clk']
    return job_links


async def extract_job_text(session, link):
    html = await fetch(session, link)
    job_soup = await get_soup(html)
    job_text = ['\t'.join([p.text for p in description.findAll(['p', 'li'])])
                for description in job_soup.findAll('div',
                                                    {'class': 'jobsearch-JobComponent-description'})]
    if len(job_text) > 0:
        job_text = job_text[0]
    else:
        job_text = ''
    return job_text


async def main(urls):
    async with aiohttp.ClientSession() as session:
        start_time = time.time()
        jobs = []
        for url in urls:
            print(url)
            # to get a sense of how long this takes
            # for any given thread,
            # I time how long it takes to go through
            start_ = time.time()
            html = await fetch(session, url[1])
            job_links = await extract_job_links(html)
            job_texts = []
            for idx, job in enumerate(job_links):
                job_text = await extract_job_text(session, job)
                # this uses the paging from the URL to determine the job posting index
                url_idx = 0 if len(url[1].split('=')[-1]) > 4 else int(url[1].split('=')[-1])
                job_texts.append((url[0], str(url_idx + idx), job, job_text))
            jobs.extend(job_texts)
            print(time.time() - start_, "seconds")
        print("Entire request took", time.time() - start_time, "seconds")
        return jobs


if __name__ == '__main__':
    # the input file is expected to be 
    # a csv file of job titles
    # to be entered as queries 
    # into the indeed.com website 

    input_file = sys.argv[1]  # ex. 'data/job_titles_part0.csv'
    output_file = sys.argv[2]  # ex. 'job_postings_2.tsv'
    homepage = 'https://www.indeed.com'

    positions = []
    with open(input_file) as file:
        positions = file.read()
    positions = [position.replace(' ', '-').replace('/', '_') for position in positions.split(',')]
    print(len(positions))

    beginning = 10
    last = 101
    # indeed.com uses paging that jumps by 10 at a time
    paging = range(beginning, last, 10)

    todo_postions = []
    data_dir = 'data/job_postings/'
    for position in positions:
        if not os.path.exists(data_dir + position):
            os.mkdir(data_dir + position)
        if len(os.listdir(data_dir + position)) == 0:
            todo_postions.append(position)

    positions = todo_postions

    # for every job title, create the equivalent query
    # for the number of pages for that job
    # ex. data-scientist-10, data-scientist-20, etc.
    # the first page is unusual and doesn't have the same structure
    # as all of the following pages 
    full_urls = [[(position, homepage + '/q-' + position + '-jobs.html')] +
                 [(position, homepage + '/jobs?q=' + position.replace('-', '+') + '&start=' + str(page))
                  for page in paging]
                 for position in positions]
    # this flattens the list of lists to a single list
    full_urls = [url for group in full_urls for url in group]
    print(len(full_urls))

    loop = asyncio.get_event_loop()
    try:
        # here I return values from the asyncio tasks
        # so that I can keep a record of the jobs
        # which I then output to files below
        task = loop.create_task(main(full_urls))
        jobs = loop.run_until_complete(task)

        print(len(jobs))
        print(jobs[0])

        for job in jobs:
            with open(data_dir + job[0] + '/' + job[0] + '_' + job[1] + '.txt', 'a') as file:
                file.write(job[3])
        with open(data_dir + output_file, 'a') as file:
            output = '\n'.join(['\t'.join(job_posting[:-1])
                                for job_posting in jobs])
            file.write(output)
    except Exception as e:
        print(e)
        pass
    finally:
        loop.close()

import argparse
import asyncio
import json
from datetime import datetime
from operator import itemgetter
from typing import Tuple, Optional, Union

from aiohttp import web
from itertools import count
from pydantic import BaseModel, ValidationError

queue = asyncio.Queue()

db = {}

ids = count(1)

STATUS_IN_QUEUE = 'In queue'
STATUS_IN_PROCESS = 'In process'


class Task(BaseModel):
    id: Optional[int]
    number_in_queue: Optional[int]
    status: Optional[str]
    count: int
    delta: float
    start: int
    interval: float
    current_value: Union[int, float, None]
    date: Union[datetime, str, None]


async def create_queue() -> asyncio.Queue:
    return asyncio.Queue()


async def get_id() -> int:
    # try:
    #     id_ = ids[-1] + 1
    # except IndexError:
    #     id_ = 1
    # ids.append(id_)
    # return id_
    return next(ids)


def serialize(data: dict) -> Tuple[Optional[Task], Optional[str]]:
    try:
        task = Task(**data)
    except ValidationError as e:
        err = str(e.json())
        return None, err
    else:
        err = None
        return task, err


async def handle_create_task(request) -> web.Response:
    task = dict(request.rel_url.query)

    task, err = serialize(task)
    if err is not None:
        return web.Response(text=err, status=400)

    task.id = await get_id()
    size = queue.qsize()
    task.number_in_queue = size + 1
    task.status = STATUS_IN_QUEUE
    db[task.id] = task
    await queue.put(task)

    return web.Response(text=task.json(), status=200)


async def logic(task: Task) -> None:
    for n in range(task.count):
        a = task.start + task.delta * n
        state = db.get(task.id)
        state.current_value = a

        await asyncio.sleep(task.interval)


async def worker() -> None:
    while True:
        task = await queue.get()
        state = db.get(task.id)
        state.status = STATUS_IN_PROCESS
        state.date = datetime.now().isoformat()
        await logic(task)
        queue.task_done()

        del db[task.id]


async def handle_progress(request) -> web.Response:
    states = [state.dict()for state in db.values()]
    return web.Response(text=json.dumps(sorted(states, key=itemgetter('id'))), status=200)


app = web.Application()
app.router.add_route('GET', '/create_task', handle_create_task)
app.router.add_route('GET', '/states', handle_progress)


async def main(workers: int) -> None:
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, 'localhost', 8000)
    await site.start()
    tasks = [asyncio.ensure_future(worker()) for _ in range(workers)]
    await asyncio.gather(*tasks)


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument("-w", "--workers", required=False,
                    help="number workers")
    args = vars(ap.parse_args())
    number_workers = int(args['workers']) if args['workers'] is not None else 3
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(number_workers))

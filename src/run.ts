import { IExecutor } from './Executor';
import ITask from './Task';

export default async function run(executor: IExecutor, queue: AsyncIterable<ITask>, maxThreads = 0){
    maxThreads = Math.max(0, maxThreads);
    const activeTargets = new Map<number,{
        current:Promise<void>|null,
        queue:ITask[]
    }>();
    const activeTasks = new Set<Promise<void>>();
    const iterator = queue[Symbol.asyncIterator]();
    let pendingTask:Promise<IteratorResult<ITask>>|null = null;

    async function getNextTask():Promise<IteratorResult<ITask>>{
        if(pendingTask){return pendingTask};
        pendingTask = iterator.next();
        const result = await pendingTask;
        pendingTask = null;
        return result;
    }

    async function processTask(targetId: number){
        const targetQueue = activeTargets.get(targetId)!;
        while(targetQueue.queue.length > 0){
            const task = targetQueue.queue.shift()!;
            targetQueue.current = (async()=>{
                try{
                    await executor.executeTask(task);
                }finally{
                    if(targetQueue.queue.length === 0){
                        activeTargets.delete(targetId);
                    }
                }
            })();
            await targetQueue.current;
        }
    }

    while(true){
        const result = await getNextTask();
        if(result.done){
            if (activeTasks.size === 0){break};
            await Promise.race(activeTasks);
            continue;
        }

        const task = result.value;
        const targetId = task.targetId;

        if(!activeTargets.has(targetId)){
            activeTargets.set(targetId,{
                current: null,
                queue: []
            });
        }

        const targetQueue = activeTargets.get(targetId)!;
        targetQueue.queue.push(task);

        if(targetQueue.current === null){
            const taskPromise = processTask(targetId);
            activeTasks.add(taskPromise);
            taskPromise.finally(() => activeTasks.delete(taskPromise));
        }

        if(maxThreads > 0 && activeTasks.size >= maxThreads){
            await Promise.race(activeTasks);
        }
    }

    await Promise.all(activeTasks);
}
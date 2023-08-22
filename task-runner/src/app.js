const { spawn } = require('child_process');
const cron = require('node-cron');
const fs = require('fs')
const path = require('path')
const { loadSecrets } = require('./aws')

cron.schedule('1 * * * *', hotloadTasks)

const scheduledTasks = {}

// todo: add timeout parameter, add option to export log to r2/s3 something with weekly cleanup

async function load() {
  console.log('Starting the app...')
  await hotloadTasks(true)
  console.log('CRONOS_RPC:', process.env.CRONOS_RPC)
}

load()

async function hotloadTasks(isFirstRun) {
  await loadSecrets()
  const { tasks } = readTasks()
  removeDelistedTasks()
  await scheduleNewTasks()

  function readTasks() {
    const rootFolder = isFirstRun ? __dirname : 'app/repo/task-runner/src'
    const file = path.join(rootFolder, process.env.TASK_FILE)
    return JSON.parse(fs.readFileSync(file))
  }

  function removeDelistedTasks() {
    for (const id of Object.keys(scheduledTasks)) {
      if (tasks[id]) continue;
      console.log('Stop scheduled task', id)
      scheduledTasks[id].stop()
      delete scheduledTasks[id]
    }
  }

  async function scheduleNewTasks() {
    for (const [id, taskObj] of Object.entries(tasks)) {
      if (scheduledTasks[id]) continue;
      console.log('Start scheduled task', id)
      const taskFn = formTaskFunction(id, taskObj)
      if (taskObj.run_on_load) await taskFn()
      scheduledTasks[id] = cron.schedule(taskObj.schedule, taskFn)
    }
  }

  function formTaskFunction(id, taskObj) {
    return async () => {
      taskObj.name = id
      if (taskObj.npm_script) return runNpmCommand(taskObj)
      return spawnPromise(taskObj)
    }
  }

  async function runNpmCommand({ npm_script, name, script_location }) {
    return spawnPromise({
      name,
      bash_script: `
        cd repo/${script_location}
        npm run ${npm_script}
      `
    })
  }

  async function spawnPromise({ bash_script, name }) {
    const start = +new Date()
    if (Array.isArray(bash_script)) bash_script = bash_script.join('\n')
    console.log('[Start]', name)
    return new Promise((resolve, reject) => {
      const childProcess = spawn('bash', ['-c', bash_script], { stdio: 'inherit' });

      childProcess.on('close', (code) => {
        const runTime = ((+(new Date) - start) / 1e3).toFixed(1)
        console.log(`[Done] ${name} | runtime: ${runTime}s  `)
        if (code === 0) {
          resolve()
        } else {
          reject(new Error(`Child process exited with code ${code}`));
        }
      });
    });
  }
}
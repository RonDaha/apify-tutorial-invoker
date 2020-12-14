/* The Amazon Actor Wrapper - will call the Amazon Actor task */

/*
* I did not added things like validation for the inputs (like make sure the memory is a power of 2 etc.) to keep things simple
* I used two 3rd party libraries, 'Axios' and 'convert-array-to-csv' without taking in account the size of the final image (again, to keep things simple)
* */
const Apify = require('apify');
const Axios = require('axios');
const { utils } = Apify
const { log }  = utils
const { convertArrayToCSV } = require('convert-array-to-csv');
const taskName = 'AmazonActor/search-phone'
const taskId = 'y8qBfIZMzSICtmWKy'

Apify.main(async () => {
    const token = process.env.TOKEN
    let { memory, useClient, fields, maxItems } = await Apify.getInput();
    let datasetId

    if (useClient) {
        log.info(`Using client to trigger task - ${taskName}`)
        const actorRun = await Apify.callTask(taskId, undefined, { memoryMbytes: memory })
        log.info(`Task is done with status - ${actorRun.status}`)
        datasetId = actorRun.defaultDatasetId
    } else {
        log.info(`Using API to trigger task - ${taskName}`)
        const actorRun = await Axios.post(`https://api.apify.com/v2/actor-tasks/${taskId}/runs?token=${token}&memory=${memory}`)
        const limit = 40 // give a limit to the pooling for safety on dev
        for (let i = 0; i < limit; i++) {
            log.info('Checking task status')
            await utils.sleep(4000)
            const taskRun = await Axios.get(`https://api.apify.com/v2/acts/${taskId}/runs/${actorRun.data.data.id}`)
            log.info(`Task status - ${taskRun.data.data.status}`)
            if (taskRun.data.data.status === 'SUCCEEDED') {
                datasetId = taskRun.data.data.defaultDatasetId
                break
            }
        }
    }
    if (!datasetId) {
        log.info('Failed to get datasetId, break process')
        return
    }
    const dataset = await Apify.openDataset(datasetId)
    const response = await dataset.getData()
    const data = response.items
    log.info(`Got ${data.length} records`)
    const relevantProducts = []

    for (const product of data) {
        if (relevantProducts.length === maxItems) {
            break
        }
        const relevantProductFields = {}
        for (const field of fields) {
            relevantProductFields[field] = product[field]
        }
        relevantProducts.push(relevantProductFields)
    }

    const store = await Apify.openKeyValueStore();
    await store.setValue('OUTPUT', convertArrayToCSV(relevantProducts), { contentType: 'text/csv' })

    log.info('Process is done')
});

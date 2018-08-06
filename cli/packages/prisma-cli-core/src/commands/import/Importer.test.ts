import * as fs from 'fs-extra'
import * as path from 'path'
import { Importer } from './Importer'
import { Environment } from 'prisma-yml'
import { Client, Config, Output } from 'prisma-cli-engine'
import { getTmpDir } from '../../../../prisma-yml/src/test/getTmpDir'

function initImporter() {
  const config = new Config()
  const output = new Output(config)
  const env = new Environment(getTmpDir(), output)
  const client = new Client(config, env, output)
  const types = `
    type Post {
      title: String
      description: String
      state: String
    }
  `
  return new Importer(
    path.join(__dirname, 'fixtures', 'basic', 'import-missing-ids'),
    types,
    client,
    output,
    config,
  )
}

describe('Importer', () => {
  const importer = initImporter()

  it('Should throw an error when ids are missing', async () => {
    await importer.upload('serviceName', 'stage')
    expect(importer.out.stdout.output).toMatchSnapshot()
  })
})

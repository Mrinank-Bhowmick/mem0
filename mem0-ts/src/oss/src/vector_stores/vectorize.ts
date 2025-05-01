import Cloudflare from "cloudflare";
import type { Vectorize, VectorizeVector } from "@cloudflare/workers-types";
import { VectorStore } from "./base";
import { SearchFilters, VectorStoreConfig, VectorStoreResult } from "../types";

interface VectorizeConfig extends VectorStoreConfig {
  dimension?: number;
  apiKey?: string;
  indexName: string;
  accountId: string;
  binding?: Vectorize; // TODO - Optional binding for Cloudflare Workers
}

const test_data: VectorizeVector = {
  id: "med1",
  values: [
    0.14, 0.23, 0.36, 0.51, 0.6, 0.47, 0.59, 0.71, 0.33, 0.89, 0.41, 0.53, 0.68,
    0.29, 0.77, 0.45, 0.24, 0.66, 0.71, 0.34, 0.86, 0.57, 0.62, 0.48, 0.78,
    0.52, 0.37, 0.61, 0.69, 0.28, 0.8, 0.52,
  ],
  metadata: {
    userId: "rakesh",
    data: "Likes going to hikes",
    hash: "4478e7d1c435ece797e7a17b82c5d776",
  },
};

interface CloudflareVector {
  id: string;
  values: number[];
  metadata?: Record<string, any>;
}

export class VectorizeDB implements VectorStore {
  private client: Cloudflare | null = null;
  private dimensions: number;
  private indexName: string;
  private accountId: string;
  private binding?: Vectorize; // TODO - Optional binding for Cloudflare Workers

  constructor(config: VectorizeConfig) {
    this.client = new Cloudflare({ apiToken: config.apiKey });
    this.dimensions = config.dimension || 1536;
    this.indexName = config.indexName;
    this.accountId = config.accountId;
    this.binding = config.binding; // TODO - Optional binding for Cloudflare Workers
  }

  async insert(
    vectors: number[][],
    ids: string[],
    payloads: Record<string, any>[]
  ): Promise<void> {
    const vectorObjects: CloudflareVector[] = vectors.map((vector, index) => ({
      id: ids[index],
      values: vector,
      metadata: payloads[index] || {},
    }));

    const ndjsonBody = vectorObjects.map((v) => JSON.stringify(v)).join("\n");

    // TODO - Optional binding for Cloudflare Workers
    if (this.binding) {
      //this.binding.insert();
    }

    console.log(
      `Inserting ${vectorObjects.length} vector(s) into index '${this.indexName}'...`
    );

    // Error in Cloudflare ts package when inserting vectors
    /*
      const response = await this.client?.vectorize.indexes.insert(
        this.indexName,
        {
          account_id: this.accountId,
          body: JSON.stringify(test_data),
          "unparsable-behavior": "error",
        }
      );
      */

    const response = await fetch(
      `https://api.cloudflare.com/client/v4/accounts/${this.accountId}/vectorize/v2/indexes/${this.indexName}/insert`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/x-ndjson",
          Authorization: `Bearer ${this.client?.apiToken}`,
        },
        body: ndjsonBody,
      }
    );

    console.log(
      "Cloudflare Vectorize Insert Response:",
      JSON.stringify(response)
    );
  }

  async search(
    query: number[],
    limit: number = 5,
    filters?: SearchFilters
  ): Promise<VectorStoreResult[]> {
    console.log(
      `Searching index '${this.indexName}' with query vector of length ${query.length}...`
    );
    // calculate time taken to execute the query
    const startTime = Date.now();
    const result = await this.client?.vectorize.indexes.query(this.indexName, {
      account_id: this.accountId,
      vector: query,
      filter: filters, // Remember user needs to create-metadata-index after making vectorize db
      returnMetadata: "all",
      topK: limit,
    });
    const endTime = Date.now();
    const timeTaken = endTime - startTime;
    console.log(`Query executed in ${timeTaken} ms`);

    console.log(
      "Cloudflare Vectorize Search Result:",
      JSON.stringify(result, null, 2)
    );

    return result?.matches?.map((match) => ({
      id: match.id,
      payload: match.metadata,
      score: match.score,
    })) as VectorStoreResult[];
  }

  async get(vectorId: string): Promise<VectorStoreResult | null> {
    const result = (await this.client?.vectorize.indexes.getByIds(
      this.indexName,
      {
        account_id: this.accountId,
        ids: [vectorId],
      }
    )) as any;

    if (!result?.length) return null;

    return {
      id: vectorId,
      payload: result[0].metadata,
    };
  }

  async update(
    vectorId: string,
    vector: number[],
    payload: Record<string, any>
  ): Promise<void> {
    throw new Error("Not yet implemented");
  }

  async delete(vectorId: string): Promise<void> {
    throw new Error("Not yet implemented");
  }

  async deleteCol(): Promise<void> {
    throw new Error("Not yet implemented");
  }

  async list(
    filters?: SearchFilters,
    limit?: number
  ): Promise<[VectorStoreResult[], number]> {
    throw new Error("Not yet implemented");
  }

  async getUserId(): Promise<string> {
    throw new Error("Not yet implemented");
  }

  async setUserId(userId: string): Promise<void> {
    throw new Error("Not yet implemented");
  }

  async initialize(): Promise<void> {
    throw new Error("Not yet implemented");
  }
}

async function testing() {
  const config: VectorizeConfig = {
    apiKey: "i9pHBxfRi471tOL8HM3LAfnVYAk0U_CUfNJBDVPb",
    accountId: "51b5d4b1862c5d3379a7b244c5593875",
    indexName: "tutorial-index",
    dimension: 32,
  };

  const vectorStore = new VectorizeDB(config);
  const samplePayload = {
    userId: "rakesh",
    data: "Likes going to hikes",
    hash: "4478e7d1c435ece797e7a17b82c5d776",
  };

  // const res = await vectorStore.search(
  //   [
  //     0.14, 0.23, 0.36, 0.51, 0.6, 0.47, 0.59, 0.71, 0.33, 0.89, 0.41, 0.53,
  //     0.68, 0.29, 0.77, 0.45, 0.24, 0.62, 0.71, 0.34, 0.86, 0.57, 0.62, 0.48,
  //     0.78, 0.52, 0.37, 0.61, 0.69, 0.28, 0.8, 0.52,
  //   ],
  //   10,
  //   {
  //     userId: "rakesh2",
  //   }
  // );

  const res = await vectorStore.get("med2");
  console.log(res);

  /*
  await vectorStore.insert(
    [
      [
        0.14, 0.23, 0.36, 0.51, 0.6, 0.47, 0.59, 0.71, 0.33, 0.89, 0.41, 0.53,
        0.68, 0.29, 0.77, 0.45, 0.24, 0.62, 0.71, 0.34, 0.86, 0.57, 0.62, 0.48,
        0.78, 0.52, 0.37, 0.61, 0.69, 0.28, 0.8, 0.52,
      ],
      [
        0.14, 0.23, 0.36, 0.51, 0.62, 0.47, 0.59, 0.74, 0.33, 0.89, 0.41, 0.53,
        0.68, 0.29, 0.77, 0.45, 0.24, 0.66, 0.71, 0.34, 0.86, 0.57, 0.62, 0.48,
        0.78, 0.52, 0.37, 0.61, 0.69, 0.28, 0.8, 0.53,
      ],
    ],
    ["131", "2313"],
    [samplePayload, samplePayload]
  );
  */
}

//testing();

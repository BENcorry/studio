// This Source Code Form is subject to the terms of the Mozilla Public
// License, v2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/
import { compare, Time } from "@foxglove/rostime";
import { MessageEvent, TopicStats, Topic } from "@foxglove/studio-base/players/types";
import { RosDatatypes } from "@foxglove/studio-base/types/RosDatatypes";

import { BagIterableSource } from "./BagIterableSource";
import {
  IIterableSource,
  IteratorResult,
  Initalization,
  MessageIteratorArgs,
  GetBackfillMessagesArgs,
  IterableSourceInitializeArgs,
} from "./IIterableSource";

type BagsSource = { type: "file"; files: File[] };
type TopicStatsMap = Map<string, TopicStats>;
type PublishersByTopic = Map<string, Set<string>>;

export class BagsIterableSource implements IIterableSource {
  private _bagIterableSourceList: BagIterableSource[] = [];
  private _itemSourceTimeLine: { start: Time; end: Time }[] = [];

  public constructor(source: BagsSource) {
    this._bagIterableSourceList = source.files.map(
      (file) => new BagIterableSource({ type: "file", file }),
    );
  }

  // build complete time line supply for Player
  public async initialize(): Promise<Initalization> {
    const initializeList = await Promise.all(
      this._bagIterableSourceList.map(async (bagIterableSource) => {
        const initializeItem = await bagIterableSource.initialize();
        return initializeItem;
      }),
    );
    return this._mergeInitialize(initializeList);
  }

  private _mergeInitialize(initializeList: Initalization[]) {
    return initializeList.reduce((returnInitialize, initializeItem) => {
      this._itemSourceTimeLine.push({ start: initializeItem.start, end: initializeItem.end });
      return {
        topics: this._mergeTopics(returnInitialize.topics, initializeItem.topics),
        topicStats: this._mergeTopicStats(returnInitialize.topicStats, initializeItem.topicStats),
        start:
          compare(returnInitialize.start, initializeItem.start) <= 0
            ? returnInitialize.start
            : initializeItem.start,
        end:
          compare(returnInitialize.end, initializeItem.end) >= 0
            ? returnInitialize.end
            : initializeItem.end,
        datatypes: this._mergeDatatypes(returnInitialize.datatypes, initializeItem.datatypes),
        profile: initializeItem.profile,
        publishersByTopic: this._MergePublishersByTopic(
          returnInitialize.publishersByTopic,
          initializeItem.publishersByTopic,
        ),
        problems: Array.from(new Set([...returnInitialize.problems, ...initializeItem.problems])),
      };
    });
  }

  private _mergeTopics(src: Topic[], from: Topic[]): Topic[] {
    from.forEach((topic) => {
      if (!src.find((srcTopic) => this._isSameTopic(srcTopic, topic))) {
        src.push(topic);
      }
    });
    return src;
  }

  private _isSameTopic(srcTopic: Topic, fromTopic: Topic): boolean {
    return srcTopic.name === fromTopic.name && srcTopic.schemaName === fromTopic.schemaName;
  }

  private _MergePublishersByTopic(
    src: PublishersByTopic,
    from: PublishersByTopic,
  ): PublishersByTopic {
    from.forEach((publishersByTopic, key) => {
      const fromPublishersByTopic = src.get(key);
      if (fromPublishersByTopic) {
        src.set(key, new Set([...fromPublishersByTopic, ...publishersByTopic]));
      } else {
        src.set(key, publishersByTopic);
      }
    });
    return src;
  }

  private _mergeDatatypes(src: RosDatatypes, from: RosDatatypes): RosDatatypes {
    from.forEach((datatype, key) => {
      const srcDatatype = src.get(key);
      if (!srcDatatype) {
        src.set(key, datatype);
      }
    });

    return src;
  }

  private _mergeTopicStats(src: TopicStatsMap, from: TopicStatsMap): TopicStatsMap {
    from.forEach((stats, key) => {
      const srcStats = src.get(key);
      if (srcStats) {
        const tempStats = {
          numMessages: srcStats.numMessages + stats.numMessages,
          firstMessageTime: srcStats.firstMessageTime ??
            stats.firstMessageTime ?? { sec: 0, nsec: 0 },
          lastMessageTime: stats.lastMessageTime ?? srcStats.lastMessageTime ?? { sec: 0, nsec: 0 },
        };
        src.set(key, tempStats);
      } else {
        src.set(key, stats);
      }
    });

    return src;
  }

  public async *messageIterator(
    opt: MessageIteratorArgs,
  ): AsyncIterableIterator<Readonly<IteratorResult>> {
    for await (const bagIterable of this._bagIterableSourceList) {
      yield* bagIterable.messageIterator({
        ...opt,
        start: opt.start,
        end: opt.end,
      });
    }
  }

  public async getBackfillMessages({
    topics,
    time,
  }: GetBackfillMessagesArgs): Promise<MessageEvent<unknown>[]> {
    const index = this._itemSourceTimeLine.findIndex((item) => compare(item.start, time) > 0);
    const currentBagIterator = this._bagIterableSourceList[
      index > -1 ? index : 0
    ] as BagIterableSource;
    return await currentBagIterator.getBackfillMessages({ topics, time });
  }
}

export function initialize(args: IterableSourceInitializeArgs): BagsIterableSource {
  if (args.files) {
    return new BagsIterableSource({ type: "file", files: args.files });
  }

  throw new Error("file or url required");
}

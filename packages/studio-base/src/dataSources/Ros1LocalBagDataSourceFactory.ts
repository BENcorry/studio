// This Source Code Form is subject to the terms of the Mozilla Public
// License, v2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/

import {
  IDataSourceFactory,
  DataSourceFactoryInitializeArgs,
} from "@foxglove/studio-base/context/PlayerSelectionContext";
import { IterablePlayer, WorkerIterableSource } from "@foxglove/studio-base/players/IterablePlayer";
import { Player } from "@foxglove/studio-base/players/types";

class Ros1LocalBagDataSourceFactory implements IDataSourceFactory {
  public id = "ros1-local-bagfile";
  public type: IDataSourceFactory["type"] = "file";
  public displayName = "ROS 1 Bag";
  public iconName: IDataSourceFactory["iconName"] = "OpenFile";
  public supportedFileTypes = [".bag"];
  public supportsMultiFile = true;

  public initialize(args: DataSourceFactoryInitializeArgs): Player | undefined {
    const files = args.file ? [args.file] : args.files;
    const name = args.file ? args.file.name : args.files?.map((file) => file.name).join(",");

    if (!files || files.length === 0) {
      return;
    }

    const source = new WorkerIterableSource({
      sourceType: files.length === 1 ? "rosbag" : "rosbags",
      initArgs: files.length === 1 ? { file: files[0] } : { files },
    });

    return new IterablePlayer({
      metricsCollector: args.metricsCollector,
      source,
      name,
      sourceId: this.id,
    });
  }
}

export default Ros1LocalBagDataSourceFactory;

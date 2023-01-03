// This Source Code Form is subject to the terms of the Mozilla Public
// License, v2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/

import path from "path";
import { useCallback, useMemo } from "react";

import {
  IDataSourceFactory,
  usePlayerSelection,
} from "@foxglove/studio-base/context/PlayerSelectionContext";
import showOpenFilePicker from "@foxglove/studio-base/util/showOpenFilePicker";

export function useOpenFile(sources: IDataSourceFactory[]): () => Promise<void> {
  const { selectSource } = usePlayerSelection();

  const allExtensions = useMemo(() => {
    return sources.reduce((all, source) => {
      if (!source.supportedFileTypes) {
        return all;
      }

      return [...all, ...source.supportedFileTypes];
    }, [] as string[]);
  }, [sources]);

  return useCallback(async () => {
    const fileHandles = await showOpenFilePicker({
      multiple: true,
      types: [
        {
          description: allExtensions.join(", "),
          accept: { "application/octet-stream": allExtensions },
        },
      ],
    });
    if (fileHandles.length === 0) {
      return;
    }

    let sourceExtName = "";
    const files = await Promise.all(
      fileHandles.map(async (fileHandle) => {
        const file = await fileHandle.getFile();
        const currentFileExtname = path.extname(file.name);
        if (sourceExtName === "") {
          sourceExtName = currentFileExtname;
        } else {
          if (sourceExtName !== currentFileExtname) {
            throw new Error(`Different file suffix.`);
          }
        }
        return file;
      }),
    );

    if (files.length === 0) {
      throw new Error(`No files selected`);
    }

    // Find the first _file_ source which can load our extension
    const matchingSources = sources.filter((source) => {
      // Only consider _file_ type sources that have a list of supported file types
      if (!source.supportedFileTypes || source.type !== "file") {
        return false;
      }

      return source.supportedFileTypes.includes(sourceExtName);
    });

    if (matchingSources.length > 1) {
      throw new Error(`Multiple source matched ${sourceExtName}. This is not supported.`);
    }

    const foundSource = matchingSources[0];
    if (!foundSource) {
      throw new Error(`Cannot find source to handle ${sourceExtName}`);
    }

    selectSource(foundSource.id, { type: "file", files });
  }, [allExtensions, selectSource, sources]);
}

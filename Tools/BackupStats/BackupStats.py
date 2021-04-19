#!/usr/bin/env python


"Display some useful stats on your remote backups"


import argparse
import sqlite3
import uuid

def analyse_backup(database_path):
    conn = sqlite3.connect(database_path)
    c = conn.cursor()

    c.execute('SELECT count(*) as nb_volumes FROM Remotevolume where type="Blocks" and VerificationCount=0')
    never_checked = c.fetchone()[0]

    print(f'\n{never_checked} blccks have never been verified since being uploaded')


def wasted_space(database_path):

    print("=========================\nGenerating storage report. Please wait...")
    conn = sqlite3.connect(database_path)
    c = conn.cursor()
    megabyte = 1024**2

    tmptablename = 'usagereport_' + uuid.uuid1().hex

    usedBlocks = 'SELECT SUM(Block.Size) AS ActiveSize, Block.VolumeID AS VolumeID FROM Block, Remotevolume WHERE Block.VolumeID = Remotevolume.ID AND Block.ID NOT IN (SELECT Block.ID FROM Block,DeletedBlock WHERE Block.Hash = DeletedBlock.Hash AND Block.Size = DeletedBlock.Size AND Block.VolumeID = DeletedBlock.VolumeID) GROUP BY Block.VolumeID '
    lastmodifiedFile = 'SELECT Block.VolumeID AS VolumeID, Fileset.Timestamp AS Sorttime FROM Fileset, FilesetEntry, FileLookup, BlocksetEntry, Block WHERE FilesetEntry.FileID = FileLookup.ID AND FileLookup.BlocksetID = BlocksetEntry.BlocksetID AND BlocksetEntry.BlockID = Block.ID AND Fileset.ID = FilesetEntry.FilesetID '
    lastmodifiedMetadata = 'SELECT Block.VolumeID AS VolumeID, Fileset.Timestamp AS Sorttime FROM Fileset, FilesetEntry, FileLookup, BlocksetEntry, Block, Metadataset WHERE FilesetEntry.FileID = FileLookup.ID AND FileLookup.MetadataID = Metadataset.ID AND Metadataset.BlocksetID = BlocksetEntry.BlocksetID AND BlocksetEntry.BlockID = Block.ID AND Fileset.ID = FilesetEntry.FilesetID '
    scantime = 'SELECT VolumeID AS VolumeID, MIN(Sorttime) AS Sorttime FROM (' + lastmodifiedFile + ' UNION ' + lastmodifiedMetadata + ') GROUP BY VolumeID '
    active = 'SELECT A.ActiveSize AS ActiveSize,  0 AS InactiveSize, A.VolumeID AS VolumeID, CASE WHEN B.Sorttime IS NULL THEN 0 ELSE B.Sorttime END AS Sorttime FROM (' + usedBlocks + ') A LEFT OUTER JOIN (' + scantime + ') B ON B.VolumeID = A.VolumeID '
    inactive = 'SELECT 0 AS ActiveSize, SUM(Size) AS InactiveSize, VolumeID AS VolumeID, 0 AS SortScantime FROM DeletedBlock GROUP BY VolumeID '
    empty = 'SELECT 0 AS ActiveSize, 0 AS InactiveSize, Remotevolume.ID AS VolumeID, 0 AS SortScantime FROM Remotevolume WHERE Remotevolume.Type = ? AND Remotevolume.State IN (?, ?) AND Remotevolume.ID NOT IN (SELECT VolumeID FROM Block) '
            
    combined = active + ' UNION ' + inactive + ' UNION ' + empty
    collected = 'SELECT VolumeID AS VolumeID, SUM(ActiveSize) AS ActiveSize, SUM(InactiveSize) AS InactiveSize, MAX(Sorttime) AS Sorttime FROM (' + combined + ') GROUP BY VolumeID '
    createtable = 'CREATE temporary TABLE ' + tmptablename + ' AS ' + collected

    c.execute(createtable, ("Blocks","Verified", "Uploaded"))

    c.execute("select count(*), sum(ActiveSize), sum(InactiveSize) from " + tmptablename)
    nb_blocks, total_active_size, total_inactive_size = c.fetchone()
    
    c.execute("select count(*), sum(ActiveSize), sum(InactiveSize) from " + tmptablename + " where InactiveSize > 0")
    nb_blocks_in_stale, active_size_in_stale, total_inactive_size_in_stale = c.fetchone()

    print(f'Total block size used: {(total_active_size + total_inactive_size) / megabyte:,.1f} MB in {nb_blocks:,} blocks')
    print(f'      o/w active data: {total_active_size / megabyte:,.1f} MB')
    print(f'    o/w inactive data: {total_inactive_size / megabyte:,.1f} MB ({total_inactive_size / (total_inactive_size + total_active_size):.2%})')

    print(f'There are {nb_blocks_in_stale:,} containing inactive data')
    print(f'on average those blocks include {total_inactive_size_in_stale / ( total_inactive_size_in_stale + active_size_in_stale):.1%} of inactive data')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Display statistics on your backup')
    parser.add_argument("path", type=str, nargs='?',
                    help="path to the database to analyse")
    args = parser.parse_args()

    wasted_space(args.path)
    analyse_backup(args.path)
    
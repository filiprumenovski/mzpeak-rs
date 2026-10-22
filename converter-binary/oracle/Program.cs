using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;
using ThermoFisher.CommonCore.Data.Business;
using ThermoFisher.CommonCore.RawFileReader;

if (args.Length != 1) throw new ArgumentException("Expected one RAW path");
using var raw = RawFileReaderAdapter.FileFactory(args[0]);
if (!raw.IsOpen || raw.IsError || raw.InAcquisition) throw new IOException("RAW is unavailable or incomplete");
raw.SelectInstrument(Device.MS, 1);
raw.IncludeReferenceAndExceptionData = true;
var first = raw.RunHeaderEx.FirstSpectrum;
var last = raw.RunHeaderEx.LastSpectrum;
using var output = new BinaryWriter(new BufferedStream(Console.OpenStandardOutput(), 1048576));
void WriteJson(object value) {
    var bytes = JsonSerializer.SerializeToUtf8Bytes(value);
    output.Write(bytes.Length); output.Write(bytes);
}
output.Write(Encoding.ASCII.GetBytes("MZRAW001"));
WriteJson(new { spectra=last-first+1, first_scan=first, last_scan=last,
    model=raw.GetInstrumentData().Model, source=args[0],
    reader_version=typeof(RawFileReaderAdapter).Assembly.GetName().Version?.ToString(),
    centroiding=false, mz_dtype="<f8", intensity_dtype="<f8" });
for (int scan=first; scan<=last; scan++) {
    var stats = raw.GetScanStatsForScanNumber(scan);
    var data = raw.GetSegmentedScanFromScanNumber(scan);
    var mz = data.Positions; var intensity = data.Intensities;
    if (mz.Length != intensity.Length) throw new InvalidDataException("Array lengths differ");
    WriteJson(new { index=scan-first, id=$"controllerType=0 controllerNumber=1 scan={scan}",
        count=mz.Length, ms_level=(int)raw.GetFilterForScanNumber(scan).MSOrder,
        representation=stats.IsCentroidScan ? "centroid" : "profile",
        retention_time_minutes=raw.RetentionTimeFromScanNumber(scan) });
    output.Write(MemoryMarshal.AsBytes(mz.AsSpan()));
    output.Write(MemoryMarshal.AsBytes(intensity.AsSpan()));
}
output.Flush();

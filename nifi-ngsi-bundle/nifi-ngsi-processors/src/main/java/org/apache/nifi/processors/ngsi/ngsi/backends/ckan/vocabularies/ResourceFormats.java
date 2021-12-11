package org.apache.nifi.processors.ngsi.ngsi.backends.ckan.vocabularies;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/*
 * CKAN resource formats
 * Extracted from: https://github.com/ckan/ckan/blob/2.9/ckan/config/resource_formats.json
 */

public class ResourceFormats {

    public static final Map<String, Format> resourceFormats = new HashMap<String, Format>();

    public static final Format ARCGIS_MAP_PREVIEW = new Format("ArcGIS Map Preview", "ArcGIS Map Preview",
            "ArcGIS Map Preview");
    public static final Format ARCGIS_MAP_SERVICE = new Format("ArcGIS Map Service", "ArcGIS Map Service",
            "ArcGIS Map Service");
    public static final Format ARCGIS_ONLINE_MAP = new Format("ArcGIS Online Map", "ArcGIS Online Map",
            "ArcGIS Online Map");
    public static final Format ATOM_FEED = new Format("Atom Feed", "Atom Feed", "application/atom+xml");
    public static final Format BIN = new Format("BIN", "Binary Data", "application/octet-stream");
    public static final Format BMP = new Format("BMP", "Bitmap Image File", "image/x-ms-bmp");
    public static final Format CSV = new Format("CSV", "Comma Separated Values File", "text/csv");
    public static final Format DCR = new Format("DCR", "Adobe Shockwave format", "application/x-director");
    public static final Format DBASE = new Format("dBase", "dBase Database", "application/x-dbf");
    public static final Format DOC = new Format("DOC", "Word Document", "application/msword");
    public static final Format DOCM = new Format("DOCM", "Word OOXML - Macro Enabled",
            "application/vnd.ms-word.document.macroEnabled.12");
    public static final Format DOCX = new Format("DOCX", "Word OOXML Document",
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document");
    public static final Format DOTM = new Format("DOTM", "Word OOXML Template - Macro Enabled",
            "application/vnd.ms-word.template.macroEnabled.12");
    public static final Format DOTX = new Format("DOTX", "Word OOXML - Template",
            "application/vnd.openxmlformats-officedocument.wordprocessingml.template");
    public static final Format Esri = new Format("Esri REST", "Esri Rest API Endpoint", "Esri REST");
    public static final Format EXE = new Format("EXE", "Windows Executable Program", "application/x-msdownload");
    public static final Format E00 = new Format("E00", " ARC/INFO interchange file format", "application/x-e00");
    public static final Format GEO_JSON = new Format("GeoJSON", "Geographic JavaScript Object Notation",
            "application/geo+json");
    public static final Format GIF = new Format("GIF", "GIF Image File", "image/gif");
    public static final Format GTFS = new Format("GTFS", "General Transit Feed Specification", null);
    public static final Format GZ = new Format("GZ", "Gzip File", "application/gzip");
    public static final Format HTML = new Format("HTML", "Web Page", "text/html");
    public static final Format ICS = new Format("ICS", "iCalendar", "text/calendar");
    public static final Format JPEG = new Format("JPEG", "JPG Image File", "image/jpeg");
    public static final Format JS = new Format("JS", "JavaScript", "application/x-javascript");
    public static final Format JSON = new Format("JSON", "JavaScript Object Notation", "application/json");
    public static final Format KML = new Format("KML", "KML File", "application/vnd.google-earth.kml+xml");
    public static final Format KMZ = new Format("KMZ", "KMZ File", "application/vnd.google-earth.kmz+xml");
    public static final Format MDB = new Format("MDB", "Access Database", "application/x-msaccess");
    public static final Format MOP = new Format("MOP", "MOPAC Input format", "chemical/x-mopac-input");
    public static final Format MP3 = new Format("MP3", "MPEG-1 Audio Layer-3", "audio/mpeg");
    public static final Format MP4 = new Format("MP4", "MPEG Video File", "video/mp4");
    public static final Format MRSID = new Format("MrSID", "MrSID", "image/x-mrsid");
    public static final Format MXD = new Format("MXD", "ESRI ArcGIS project file", "application/x-mxd");
    public static final Format NET_CDF = new Format("NetCDF", "NetCDF File", "application/netcdf");
    public static final Format N3 = new Format("N3", "N3 Triples", "application/x-n3");
    public static final Format OGG = new Format("OGG", "Ogg Vorbis Audio File", "audio/ogg");
    public static final Format ODB = new Format("ODB", "OpenDocument Database",
            "application/vnd.oasis.opendocument.database");
    public static final Format ODC = new Format("ODC", "OpenDocument Chart",
            "application/vnd.oasis.opendocument.chart");
    public static final Format ODF = new Format("ODF", "OpenDocument Math Formula",
            "application/vnd.oasis.opendocument.formula");
    public static final Format ODG = new Format("ODG", "OpenDocument Image",
            "application/vnd.oasis.opendocument.graphics");
    public static final Format OWL = new Format("OWL", "Web Ontology Language", "application/owl+xml");
    public static final Format ODP = new Format("ODP", "OpenDocument Presentation",
            "application/vnd.oasis.opendocument.presentation");
    public static final Format ODS = new Format("ODS", "OpenDocument Spreadsheet",
            "application/vnd.oasis.opendocument.spreadsheet");
    public static final Format ODT = new Format("ODT", "OpenDocument Text", "application/vnd.oasis.opendocument.text");
    public static final Format PDF = new Format("PDF", "PDF File", "application/pdf");
    public static final Format PERL = new Format("Perl", "Perl Script", "text/x-perl");
    public static final Format PNG = new Format("PNG", "PNG Image File", "image/png");
    public static final Format POTM = new Format("POTM", "PowerPoint Open XML - Macro-Enabled Template",
            "application/vnd.ms-powerpoint.template.macroEnabled.12");
    public static final Format POTX = new Format("POTX", "PowerPoint OOXML Presentation - Template",
            "application/vnd.openxmlformats-officedocument.presentationml.template");
    public static final Format PPAM = new Format("PPAM", "PowerPoint Open XML - Macro-Enabled Add-In",
            "application/vnd.ms-powerpoint.addin.macroEnabled.12");
    public static final Format PPT = new Format("PPT", "Powerpoint Presentation", "application/vnd.ms-powerpoint");
    public static final Format PPTM = new Format("PPTM", "PowerPoint Open XML - Macro-Enabled",
            "application/vnd.ms-powerpoint.presentation.macroEnabled.12");
    public static final Format PPTX = new Format("PPTX", "Powerpoint OOXML Presentation",
            "application/vnd.openxmlformats-officedocument.presentationml.presentation");
    public static final Format PPSM = new Format("PPSM", "PowerPoint Open XML - Macro-Enabled Slide Show",
            "application/vnd.ms-powerpoint.slideshow.macroEnabled.12");
    public static final Format PPSX = new Format("PPSX", "PowerPoint Open XML - Slide Show",
            "application/vnd.openxmlformats-officedocument.presentationml.slideshow");
    public static final Format QGIS = new Format("QGIS", "QGIS File", "application/x-qgis");
    public static final Format RAR = new Format("RAR", "RAR Compressed File", "application/rar");
    public static final Format RDF = new Format("RDF", "RDF", "application/rdf+xml");
    public static final Format RSS = new Format("RSS", "RSS feed", "application/rss+xml");
    public static final Format SHP = new Format("SHP", "Shapefile", null);
    public static final Format SPARQL = new Format("SPARQL", "SPARQL end-point", "application/sparql-results+xml");
    public static final Format SVG = new Format("SVG", "SVG vector image", "image/svg+xml");
    public static final Format TAR = new Format("TAR", "TAR Compressed File", "application/x-tar");
    public static final Format TIFF = new Format("TIFF", "TIFF Image File", "image/tiff");
    public static final Format TORRENT = new Format("TORRENT", "Torrent", "application/x-bittorrent");
    public static final Format TSV = new Format("TSV", "Tab Separated Values File", "text/tab-separated-values");
    public static final Format TXT = new Format("TXT", "Text File", "text/plain");
    public static final Format WAV = new Format("WAV", "Wave file", "audio/x-wav");
    public static final Format WCS = new Format("WCS", "Web Coverage Service", "wcs");
    public static final Format WEBM = new Format("WebM", "WEBM video", "video/webm");
    public static final Format WFS = new Format("WFS", "Web Feature Service", null);
    public static final Format WMS = new Format("WMS", "Web Mapping Service", "WMS");
    public static final Format WMTS = new Format("WMTS", "Web Map Tile Service", null);
    public static final Format XLAM = new Format("XLAM", "Excel OOXML Spreadsheet - Macro-Enabled Add-In",
            "application/vnd.ms-excel.addin.macroEnabled.12");
    public static final Format XLS = new Format("XLS", "Excel Document", "application/vnd.ms-excel");
    public static final Format XLSB = new Format("XLSB", "Excel OOXML Spreadsheet - Binary Workbook",
            "application/vnd.ms-excel.sheet.binary.macroEnabled.12");
    public static final Format XLSM = new Format("XLSM", "Excel OOXML Spreadsheet - Macro-Enabled",
            "application/vnd.ms-excel.sheet.macroEnabled.12");
    public static final Format XLSX = new Format("XLSX", "Excel OOXML Spreadsheet",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
    public static final Format XLTM = new Format("XLTM", "Excel OOXML Spreadsheet - Macro-Enabled Template",
            "application/vnd.ms-excel.template.macroEnabled.12");
    public static final Format XLTX = new Format("XLTX", "Excel OOXML Spreadsheet - Template",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.template");
    public static final Format XML = new Format("XML", "XML File", "application/xml");
    public static final Format XSLT = new Format("XSLT", "Extensible Stylesheet Language Transformations",
            "application/xslt+xml");
    public static final Format XYZ = new Format("XYZ", "XYZ Chemical File", "chemical/x-xyz");
    public static final Format ZIP = new Format("ZIP", "Zip File", "application/zip");

    static {
        resourceFormats.put(ARCGIS_MAP_PREVIEW.getName(), ARCGIS_MAP_PREVIEW);
        resourceFormats.put(ARCGIS_MAP_SERVICE.getName(), ARCGIS_MAP_SERVICE);
        resourceFormats.put(ARCGIS_ONLINE_MAP.getName(), ARCGIS_ONLINE_MAP);
        resourceFormats.put(ATOM_FEED.getName(), ATOM_FEED);
        resourceFormats.put(BIN.getName(), BIN);
        resourceFormats.put(BMP.getName(), BMP);
        resourceFormats.put(CSV.getName(), CSV);
        resourceFormats.put(DCR.getName(), DCR);
        resourceFormats.put(DBASE.getName(), DBASE);
        resourceFormats.put(DOC.getName(), DOC);
        resourceFormats.put(DOCM.getName(), DOCM);
        resourceFormats.put(DOCX.getName(), DOCX);
        resourceFormats.put(DOTM.getName(), DOTM);
        resourceFormats.put(DOTX.getName(), DOTX);
        resourceFormats.put(Esri.getName(), Esri);
        resourceFormats.put(EXE.getName(), EXE);
        resourceFormats.put(E00.getName(), E00);
        resourceFormats.put(GEO_JSON.getName(), GEO_JSON);
        resourceFormats.put(GIF.getName(), GIF);
        resourceFormats.put(GTFS.getName(), GTFS);
        resourceFormats.put(GZ.getName(), GZ);
        resourceFormats.put(HTML.getName(), HTML);
        resourceFormats.put(ICS.getName(), ICS);
        resourceFormats.put(JPEG.getName(), JPEG);
        resourceFormats.put(JS.getName(), JS);
        resourceFormats.put(JSON.getName(), JSON);
        resourceFormats.put(KML.getName(), KML);
        resourceFormats.put(KMZ.getName(), KMZ);
        resourceFormats.put(MDB.getName(), MDB);
        resourceFormats.put(MOP.getName(), MOP);
        resourceFormats.put(MP3.getName(), MP3);
        resourceFormats.put(MP4.getName(), MP4);
        resourceFormats.put(MRSID.getName(), MRSID);
        resourceFormats.put(MXD.getName(), MXD);
        resourceFormats.put(NET_CDF.getName(), NET_CDF);
        resourceFormats.put(N3.getName(), N3);
        resourceFormats.put(OGG.getName(), OGG);
        resourceFormats.put(ODB.getName(), ODB);
        resourceFormats.put(ODC.getName(), ODC);
        resourceFormats.put(ODF.getName(), ODF);
        resourceFormats.put(ODG.getName(), ODG);
        resourceFormats.put(OWL.getName(), OWL);
        resourceFormats.put(ODP.getName(), ODP);
        resourceFormats.put(ODS.getName(), ODS);
        resourceFormats.put(ODT.getName(), ODT);
        resourceFormats.put(PDF.getName(), PDF);
        resourceFormats.put(PERL.getName(), PERL);
        resourceFormats.put(PNG.getName(), PNG);
        resourceFormats.put(POTM.getName(), POTM);
        resourceFormats.put(POTX.getName(), POTX);
        resourceFormats.put(PPAM.getName(), PPAM);
        resourceFormats.put(PPT.getName(), PPT);
        resourceFormats.put(PPTM.getName(), PPTM);
        resourceFormats.put(PPTX.getName(), PPTX);
        resourceFormats.put(PPSM.getName(), PPSM);
        resourceFormats.put(PPSX.getName(), PPSX);
        resourceFormats.put(QGIS.getName(), QGIS);
        resourceFormats.put(RAR.getName(), RAR);
        resourceFormats.put(RDF.getName(), RDF);
        resourceFormats.put(RSS.getName(), RSS);
        resourceFormats.put(SHP.getName(), SHP);
        resourceFormats.put(SPARQL.getName(), SPARQL);
        resourceFormats.put(SVG.getName(), SVG);
        resourceFormats.put(TAR.getName(), TAR);
        resourceFormats.put(TIFF.getName(), TIFF);
        resourceFormats.put(TORRENT.getName(), TORRENT);
        resourceFormats.put(TSV.getName(), TSV);
        resourceFormats.put(TXT.getName(), TXT);
        resourceFormats.put(WAV.getName(), WAV);
        resourceFormats.put(WCS.getName(), WCS);
        resourceFormats.put(WEBM.getName(), WEBM);
        resourceFormats.put(WFS.getName(), WFS);
        resourceFormats.put(WMS.getName(), WMS);
        resourceFormats.put(WMTS.getName(), WMTS);
        resourceFormats.put(XLAM.getName(), XLAM);
        resourceFormats.put(XLS.getName(), XLS);
        resourceFormats.put(XLSB.getName(), XLSB);
        resourceFormats.put(XLSM.getName(), XLSM);
        resourceFormats.put(XLSX.getName(), XLSX);
        resourceFormats.put(XLTM.getName(), XLTM);
        resourceFormats.put(XLTX.getName(), XLTX);
        resourceFormats.put(XML.getName(), XML);
        resourceFormats.put(XSLT.getName(), XSLT);
        resourceFormats.put(XYZ.getName(), XYZ);
        resourceFormats.put(ZIP.getName(), ZIP);
    }

    /**
     * Constructor. It is private since utility classes should not have public or
     * default constructor
     */
    private ResourceFormats() {

    }; // ResourceFormats

    public static Set<String> getFormatNames(){
        Set<String> formatNames = new TreeSet<>();
        for (final Format format : resourceFormats.values()){
            if (format.getMimetype() != null){
                formatNames.add(format.getName());
            }
        }
        return formatNames;
    } // getFormatNames

    public static Set<String> getMimetypes(){
        Set<String> mimetypes = new TreeSet<>();
        for (final Format format : resourceFormats.values()){
            if (format.getMimetype() != null){
                mimetypes.add(format.getMimetype());
            }
        }
        return mimetypes;

    }

    private static class Format {
        private String name;
        private String humanName;
        private String mimetype;

        private Format(String name, String humanName, String mimetype) {
            this.name = name;
            this.humanName = humanName;
            this.mimetype = mimetype;
        }

        private String getName() {
            return this.name;
        }

        private String getHumanName() {
            return this.humanName;
        }

        private String getMimetype() {
            return this.mimetype;
        }

    } // Format

} // ResourceFormats

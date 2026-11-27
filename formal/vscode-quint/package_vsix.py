"""Build the local syntax extension into formal/build/ using the standard library."""

import json
from pathlib import Path
from xml.etree import ElementTree as ET
from zipfile import ZipFile, ZipInfo


SOURCE = Path(__file__).resolve().parent
FILES = ("package.json", "language-configuration.json", "syntaxes/quint.tmLanguage.json")


def vsix_manifest(package):
    root = ET.Element("PackageManifest", {
        "Version": "2.0.0",
        "xmlns": "http://schemas.microsoft.com/developer/vsx-schema/2011",
    })
    metadata = ET.SubElement(root, "Metadata")
    ET.SubElement(metadata, "Identity", Language="en-US", Id=package["name"],
                  Version=package["version"], Publisher=package["publisher"])
    ET.SubElement(metadata, "DisplayName").text = package["displayName"]
    ET.SubElement(metadata, "Description").text = package["description"]
    properties = ET.SubElement(metadata, "Properties")
    ET.SubElement(properties, "Property", Id="Microsoft.VisualStudio.Code.Engine",
                  Value=package["engines"]["vscode"])
    installation = ET.SubElement(root, "Installation")
    ET.SubElement(installation, "InstallationTarget", Id="Microsoft.VisualStudio.Code")
    ET.SubElement(root, "Dependencies")
    assets = ET.SubElement(root, "Assets")
    ET.SubElement(assets, "Asset", Type="Microsoft.VisualStudio.Code.Manifest",
                  Path="extension/package.json", Addressable="true")
    return ET.tostring(root, encoding="utf-8", xml_declaration=True)


def main():
    payload = {f"extension/{name}": (SOURCE / name).read_bytes() for name in FILES}
    package = json.loads(payload["extension/package.json"])
    payload["extension.vsixmanifest"] = vsix_manifest(package)
    payload["[Content_Types].xml"] = b"""<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="json" ContentType="application/json"/>
  <Default Extension="vsixmanifest" ContentType="text/xml"/>
</Types>"""

    output = SOURCE.parent / "build" / f"{package['name']}-{package['version']}.vsix"
    output.parent.mkdir(exist_ok=True)
    with ZipFile(output, "w") as archive:
        for name, contents in payload.items():
            # ZipInfo uses a fixed timestamp so local rebuilds can be compared.
            archive.writestr(ZipInfo(name), contents)
    print(output)


if __name__ == "__main__":
    main()

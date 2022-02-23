#!/usr/bin/python3
import os
import xml.etree.ElementTree as ET
import pprint
import requests
from urllib.parse import urlparse
from enum import Enum
from multiprocessing import Pool, cpu_count
import shutil
import subprocess

multithread = True

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
REDFISH_SCHEMA_DIR = os.path.join(SCRIPT_DIR, "csdl")
GRPC_DIR = os.path.join(SCRIPT_DIR, "grpc")
PROTO_OUT_DIR = os.path.join(GRPC_DIR, "proto_out")


# The current mechanisms of generation have problems with circular imports.
# Will likely need a way in the future to set an explicit max depth
circular_imports = [
    "SubProcessors",
    "AllocatedPools",
    "CapacitySources",
    "StorageGroups",
    "Steps",
    "MetricReportDefinition",
    "SubTasks",
    "DataProtectionLinesOfService",
]


class BaseType(Enum):
    STRING = 1
    BOOLEAN = 2
    DURATION = 3
    TIME = 4
    FLOAT = 5
    INT64 = 6
    INT32 = 7
    DECIMAL = 8
    GUID = 9


def basetype_from_edm(edm_type):
    if edm_type == "Edm.String":
        return BaseType.STRING
    if edm_type == "Edm.Boolean":
        return BaseType.BOOLEAN
    if edm_type == "Edm.Decimal":
        return BaseType.DECIMAL
    if edm_type == "Edm.Int64":
        return BaseType.INT64
    if edm_type == "Edm.Int32":
        return BaseType.INT32
    if edm_type == "Edm.DateTimeOffset":
        return BaseType.TIME
    if edm_type == "Edm.Duration":
        return BaseType.DURATION
    if edm_type == "Edm.Guid":
        return BaseType.GUID
    return None


def basetype_to_grpc(basetype):
    if basetype == BaseType.STRING:
        return "string", []
    if basetype == BaseType.BOOLEAN:
        return "bool", []
    if basetype == BaseType.DECIMAL:
        return "double", []
    if basetype == BaseType.INT64:
        return "int64", []
    if basetype == BaseType.INT32:
        return "int32", []
    if basetype == BaseType.TIME:
        return "google.protobuf.Timestamp", ["google/protobuf/timestamp.proto"]
    if basetype == BaseType.DURATION:
        return "google.protobuf.Duration", ["google/protobuf/duration.proto"]
    if basetype == BaseType.GUID:
        # TODO(ed) does grpc have a better type?
        return "string", []
    else:
        print("Can't find type for {}")


class EntityType:
    def __init__(self, name, basetype, basetype_flat, namespace, abstract, from_file):
        self.name = name
        self.properties = []
        self.basetype = basetype
        self.basetype_flat = basetype_flat
        self.namespace = namespace
        self.abstract = abstract
        self.from_file = from_file


class Enum:
    def __init__(self, name, values, namespace, from_file):
        self.name = name
        self.values = values
        self.namespace = namespace
        self.from_file = from_file


class Complex:
    def __init__(self, name, namespace, from_file):
        self.name = name
        self.namespace = namespace
        self.from_file = from_file


class TypeDef:
    def __init__(self, name, basetype, namespace, from_file):
        self.name = name
        self.basetype = basetype
        self.namespace = namespace
        self.from_file = from_file


class Collection:
    def __init__(self, name, contained_type, from_file):
        self.name = name
        self.contained_type = contained_type
        self.from_file = from_file


class PropertyPermissions(Enum):
    READ_ONLY = 0
    READ_WRITE = 1


class Property:
    def __init__(
        self, name, thistype, permissions, description, long_description, from_file
    ):
        self.name = name
        self.type = thistype
        self.permissions = permissions
        self.description = description
        self.long_description = long_description
        self.from_file = from_file


class NavigationProperty:
    def __init__(
        self,
        name,
        thistype,
        permissions,
        auto_expand,
        expand_references,
        description,
        long_description,
        from_file,
        contains_target,
    ):
        self.name = name
        self.type = thistype
        self.permissions = permissions
        self.auto_expand = auto_expand
        self.expand_references = expand_references
        self.description = description
        self.long_description = long_description
        self.from_file = from_file
        self.contains_target = contains_target


def find_element_in_scope(element_name, references, this_file):
    if element_name.startswith("Collection(") and element_name.endswith(")"):
        contained_element = find_element_in_scope(
            element_name[11:-1], references, this_file
        )
        return Collection(element_name, contained_element, this_file)

    edmtype = basetype_from_edm(element_name)
    if edmtype is not None:
        return edmtype

    for reference_uri, namespaces in references:
        uri = urlparse(reference_uri)

        filepath = os.path.join(REDFISH_SCHEMA_DIR, os.path.basename(uri.path))
        if not os.path.exists(filepath):
            print("File doesn't exist, downloading from {}".format(reference_uri))
            r = requests.get(reference_uri)
            r.raise_for_status()
            open(filepath, "wb").write(r.content)

        elements = parse_file(filepath, namespaces, element_name)

        if len(elements) == 0:
            continue
        if len(elements) > 1:
            print(
                "Found {} {} elements with referencelist {}",
                len(elements),
                element_name,
                pprint.PrettyPrinter(indent=4).pprint(references),
            )
            continue
        return elements[0]

    # finish by searching the file we're in now
    elements = parse_file(this_file, namespaces, element_name)
    if len(elements) != 1:
        return None
    return elements[0]

    print("Unable to find {}".format(element_name))
    return None


xml_cache = {}


def parse_file(filename, namespaces_to_check=[], element_name_filter=None):

    root = xml_cache.get(filename, None)
    if root is None:
        tree = ET.parse(filename)
        root = tree.getroot()
        xml_cache[filename] = root

    # list of references and namespaces
    references = []

    EntityTypes = []

    for reference in root.findall(
        "{http://docs.oasis-open.org/odata/ns/edmx}Reference"
    ):
        uri = reference.attrib["Uri"]
        namespaces = []
        for include in root.findall(
            "{http://docs.oasis-open.org/odata/ns/edmx}Include"
        ):
            ns = include.attrib["Namespace"]
            alias = include.attrib.get("Alias", ns)
            namespaces.append((ns, alias))
        references.append((uri, namespaces))

    data_services = root.findall(
        "{http://docs.oasis-open.org/odata/ns/edmx}DataServices"
    )
    for ds in data_services:
        for element in ds:
            if element.tag == "{http://docs.oasis-open.org/odata/ns/edm}Schema":
                namespace = element.attrib["Namespace"]
                if len(namespaces_to_check) == 0 or namespace in namespaces_to_check:
                    for schema_element in element:
                        name = schema_element.attrib.get("Name", None)
                        if name != None:
                            scoped_name = namespace + "." + name
                        else:
                            scoped_name = ""

                        # TODO(ed) It would be better if name, and scopename were
                        # combined so this was one search instead of two
                        if (
                            element_name_filter is not None
                            and name != element_name_filter
                            and scoped_name != element_name_filter
                        ):
                            continue

                        if (
                            schema_element.tag
                            == "{http://docs.oasis-open.org/odata/ns/edm}EntityType"
                        ):
                            basetypename = schema_element.attrib.get("BaseType", None)
                            abstract = (
                                schema_element.attrib.get("Abstract", "false") == "true"
                            )
                            if basetypename is not None:
                                basetype = find_element_in_scope(
                                    basetypename, references, filename
                                )
                                if basetype is None:
                                    print(
                                        "Unable to find basetype {}".format(
                                            basetypename
                                        )
                                    )
                            else:
                                basetype = None
                            basetype_flat = []
                            if basetype is not None:
                                basetype_flat.append(basetype)
                                if isinstance(basetype, EntityType):
                                    basetype_flat.extend(basetype.basetype_flat)
                            entity = EntityType(
                                name,
                                basetype,
                                basetype_flat,
                                namespace,
                                abstract,
                                filename,
                            )

                            for property_element in schema_element:
                                permission = PropertyPermissions.READ_WRITE
                                description = ""
                                long_description = ""
                                if (
                                    property_element.tag
                                    == "{http://docs.oasis-open.org/odata/ns/edm}Property"
                                ):
                                    prop_type = property_element.attrib["Type"]
                                    property_entity = find_element_in_scope(
                                        prop_type, references, filename
                                    )
                                    if property_entity is None:
                                        print(
                                            "Unable to find type for {}".format(
                                                prop_type
                                            )
                                        )
                                    for child in property_element:
                                        if (
                                            child.tag
                                            == "{http://docs.oasis-open.org/odata/ns/edm}Annotation"
                                        ):
                                            term = child.attrib.get("Term", "")
                                            if term == "OData.Permissions":
                                                perm = child.attrib.get(
                                                    "EnumMember", ""
                                                )
                                                if perm == "OData.Permission/Read":
                                                    permission = (
                                                        PropertyPermissions.READ_ONLY
                                                    )
                                            elif term == "OData.Description":
                                                description = child.attrib.get(
                                                    "String", ""
                                                )
                                            elif term == "OData.LongDescription":
                                                long_description = child.attrib.get(
                                                    "String", ""
                                                )
                                    # TODO(ed) subprocessor has a circular import
                                    if (
                                        property_element.attrib["Name"]
                                        in circular_imports
                                    ):
                                        pass
                                    else:
                                        entity.properties.append(
                                            Property(
                                                property_element.attrib["Name"],
                                                property_entity,
                                                permission,
                                                description,
                                                long_description,
                                                filename,
                                            )
                                        )
                                elif (
                                    property_element.tag
                                    == "{http://docs.oasis-open.org/odata/ns/edm}NavigationProperty"
                                ):
                                    expand_references = False
                                    auto_expand = False
                                    prop_type = property_element.attrib["Type"]
                                    property_entity = find_element_in_scope(
                                        prop_type, references, filename
                                    )
                                    contains_target = (
                                        property_element.attrib.get(
                                            "ContainsTarget", "false"
                                        )
                                        == "true"
                                    )
                                    if property_entity is None:
                                        print(
                                            "Unable to find type for {}".format(
                                                prop_type
                                            )
                                        )
                                    for child in property_element:
                                        term = child.attrib.get("Term", "")
                                        if term == "OData.AutoExpandReferences":
                                            expand_references = True
                                        elif term == "OData.AutoExpand":
                                            auto_expand = True
                                        elif term == "OData.Permissions":
                                            perm = child.attrib.get("EnumMember", "")
                                            if perm == "OData.Permission/Read":
                                                permission = (
                                                    PropertyPermissions.READ_ONLY
                                                )
                                        elif term == "OData.Description":
                                            description = child.attrib.get("String", "")
                                        elif term == "OData.LongDescription":
                                            long_description = child.attrib.get(
                                                "String", ""
                                            )
                                    # TODO(ed) subprocessor has a circular import
                                    if (
                                        property_element.attrib["Name"]
                                        in circular_imports
                                    ):
                                        pass
                                    else:
                                        entity.properties.append(
                                            NavigationProperty(
                                                property_element.attrib["Name"],
                                                property_entity,
                                                permission,
                                                auto_expand,
                                                expand_references,
                                                description,
                                                long_description,
                                                filename,
                                                contains_target,
                                            )
                                        )

                            EntityTypes.append(entity)

                            # print("{} {}".format(namespace, name))
                        if (
                            schema_element.tag
                            == "{http://docs.oasis-open.org/odata/ns/edm}EnumType"
                        ):
                            enums = []
                            for member in schema_element.findall(
                                "{http://docs.oasis-open.org/odata/ns/edm}Member"
                            ):
                                enums.append(member.attrib["Name"])

                            EntityTypes.append(Enum(name, enums, namespace, filename))
                        if (
                            schema_element.tag
                            == "{http://docs.oasis-open.org/odata/ns/edm}ComplexType"
                        ):
                            EntityTypes.append(Complex(name, namespace, filename))

                        if (
                            schema_element.tag
                            == "{http://docs.oasis-open.org/odata/ns/edm}TypeDefinition"
                        ):
                            underlying_type = schema_element.attrib["UnderlyingType"]

                            typedef_entity = find_element_in_scope(
                                underlying_type, references, filename
                            )
                            EntityTypes.append(
                                TypeDef(name, typedef_entity, namespace, filename)
                            )

    return EntityTypes


def parse_toplevel(filepath):
    print("Parsing {}".format(filepath))
    return parse_file(filepath)


def get_grpc_filename_from_entity(entity):
    new_filename = os.path.splitext(os.path.basename(entity.from_file))[0]
    filepath = os.path.join(new_filename, entity.name + ".proto")
    return filepath


def get_grpc_property_type_string(object_type, this_package):
    required_imports = []
    if isinstance(object_type, BaseType):
        return basetype_to_grpc(object_type)
    if isinstance(object_type, TypeDef):
        return get_grpc_property_type_string(object_type.basetype, this_package)
    if isinstance(object_type, Collection):
        text, imports = get_grpc_property_type_string(
            object_type.contained_type, this_package
        )
        return "repeated " + text, imports

    filename = get_grpc_filename_from_entity(object_type)

    required_imports.append(filename)
    if this_package == object_type.namespace.split(".")[0]:
        return object_type.name, required_imports
    return (
        "." + object_type.namespace.split(".")[0] + "." + object_type.name,
        required_imports,
    )


def generate_properties_for_entity(typedef, index_start, message_name, package_name):
    grpc_out = ""
    required_imports = []
    property_index = index_start
    if isinstance(typedef, EntityType):
        if typedef.basetype is not None:
            text, includes, property_index = generate_properties_for_entity(
                typedef.basetype, index_start, message_name, package_name
            )
            grpc_out += text
            required_imports.extend(includes)

        if len(typedef.properties) != 0:
            if property_index != 1:
                grpc_out += "\n"
            grpc_out += "    // from {}.{}\n".format(typedef.namespace, typedef.name)

        for property_obj in typedef.properties:
            if isinstance(property_obj, NavigationProperty) and not (
                property_obj.contains_target or property_obj.auto_expand
            ):
                grpc_type = "NavigationReference"
                if isinstance(property_obj.type, Collection):
                    grpc_type = "repeated " + grpc_type
                grpc_out += "    {} {} = {};\n".format(
                    grpc_type, property_obj.name, property_index
                )
                required_imports.append("NavigationReference.proto")

            else:
                text, imports = get_grpc_property_type_string(
                    property_obj.type, package_name
                )
                grpc_out += "    {} {} = {};\n".format(
                    text, property_obj.name, property_index
                )
                required_imports.extend(imports)
            property_index += 1

    return grpc_out, required_imports, property_index


def generate_grpc_for_type(typedef):
    filepath = get_grpc_filename_from_entity(typedef)
    # make the path absolute
    filepath = os.path.join(GRPC_DIR, filepath)
    try:
        os.makedirs(os.path.dirname(filepath))
    except FileExistsError:
        pass

    required_imports = []

    grpc_out = ""

    package_name = typedef.namespace.split(".")[0]

    message_name = typedef.name
    if isinstance(typedef, EntityType):
        grpc_out += "message {} {{\n".format(message_name)

        text, includes, _last_index = generate_properties_for_entity(
            typedef, 1, message_name, package_name
        )
        grpc_out += text
        required_imports.extend(includes)
        grpc_out += "}"
    elif isinstance(typedef, Enum):
        grpc_out += "enum {} {{\n".format(message_name)
        for prop_index, member in enumerate(typedef.values):
            grpc_out += "    {}_{} = {};\n".format(typedef.name, member, prop_index)
        grpc_out += "}"
    elif isinstance(typedef, Complex):
        grpc_out += "message {}{{\n".format(message_name)
        grpc_out += "    map<string, google.protobuf.Any> {} = 1;\n".format(
            message_name
        )
        grpc_out += "}\n\n"
        grpc_out += ""
        required_imports.append("google/protobuf/any.proto")
    else:
        print("Unsure how to generate for type ".format(type(typedef)))

    # sort and deduplicate
    required_imports = sorted(set(required_imports), key=str.casefold)

    file_out = 'syntax = "proto3";\n\n'
    file_out += "package {};\n\n".format(package_name)

    for import_name in required_imports:
        file_out += 'import "{}";\n'.format(import_name)
    if len(required_imports) != 0:
        file_out += "\n"
    file_out += grpc_out

    with open(filepath, "w") as grpc_file:
        grpc_file.write(file_out)
    write_meson_file_for_proto(filepath)


def write_fixed_messages():
    nav_out = 'syntax = "proto3";\n\n'
    nav_out += "message NavigationReference {\n"
    nav_out += "    string id = 1;\n"
    nav_out += "}"

    filepath = os.path.join(GRPC_DIR, "NavigationReference.proto")
    with open(filepath, "w") as nav:
        nav.write(nav_out)
    write_meson_file_for_proto(filepath)


def clear_and_make_output_dirs():
    if os.path.exists(GRPC_DIR):
        for f in os.listdir(GRPC_DIR):
            filepath = os.path.join(GRPC_DIR, f)
            if not os.path.isdir(filepath):
                os.remove(filepath)
            else:
                shutil.rmtree(filepath)
    else:
        os.makedirs(GRPC_DIR)

    if os.path.exists(PROTO_OUT_DIR):
        for f in os.listdir(PROTO_OUT_DIR):
            fullpath = os.path.join(PROTO_OUT_DIR, f)
            if os.path.isdir(fullpath):
                shutil.rmtree(fullpath)
            else:
                os.remove(fullpath)
    else:
        os.makedirs(PROTO_OUT_DIR)


def get_properties_for_service_root(entity, path="", depth=0, collectionlist=[]):
    body = ""
    header = []
    messages = ""

    # TODO(ed) What do collections look like?
    if isinstance(entity, Collection):
        collectionlist = list(collectionlist)
        collectionlist.append(path)
        return get_properties_for_service_root(
            entity.contained_type, path, 0, collectionlist
        )

    if isinstance(entity, TypeDef):
        # Note, because typedefs aren't a real "property" they don't increase
        # depth
        return get_properties_for_service_root(entity.basetype, path, 0, collectionlist)

    if isinstance(entity, EntityType):
        if path == "":
            path = "ServiceRoot"

        # only generate service definition for root level objects
        if depth == 0:
            messages += "\n"
            messages += "message Get_{}_FilterSpec{{\n".format(path)
            messages += "    string expand = 1;\n"
            messages += "    repeated string filter = 2;\n"

            for element_index, element in enumerate(collectionlist):
                messages += "    NavigationReference {}Id = {};\n".format(
                    path.split("_")[-1], element_index + 3
                )

                # TODO(ed) Figure out how to name routes with multiple IDs
                break

            messages += "}\n"

            body += (
                "    rpc Get_{0}(Get_{0}_FilterSpec) returns ({1}.{2}) {{}};\n".format(
                    path, entity.namespace.split(".")[0], entity.name
                )
            )
            filename = get_grpc_filename_from_entity(entity)
            header.append('import "{}";\n'.format(filename))

        if entity.basetype != None:
            this_body, this_header, this_messages = get_properties_for_service_root(
                entity.basetype, path, depth + 1, collectionlist
            )
            body += this_body
            header.extend(this_header)
            messages += this_messages

        for property_obj in entity.properties:
            if not isinstance(property_obj, NavigationProperty):
                continue
            new_path = path + "_" + property_obj.name
            if new_path.startswith("_"):
                new_path = new_path[1:]

            body += "\n    // from {}\n".format(entity.namespace + "." + entity.name)

            this_body, this_header, this_messages = get_properties_for_service_root(
                property_obj.type, new_path, 0, collectionlist
            )
            body += this_body
            header.extend(this_header)
            messages += this_messages

    return body, header, messages


def write_service_root(flat_list):
    service_root = [x for x in flat_list if x.name == "ServiceRoot"]
    if len(service_root) != 1:
        raise Exception("Unable to find unique service root")
    proto_filename = os.path.join(GRPC_DIR, "entry.proto")
    with open(proto_filename, "w") as service_file:
        service_file.write('syntax = "proto3";\n\n')
        service_file.write("package redfish_v1;\n\n")

        body, header, messages = get_properties_for_service_root(service_root[0])

        header.append('import "NavigationReference.proto";\n')
        # deduplicate headers
        header = sorted(set(header), key=str.casefold)
        for header_element in header:
            service_file.write(header_element)

        service_file.write(messages)

        service_file.write("service Redfish_v1{\n")
        service_file.write(body)
        service_file.write("}")
    write_meson_file_for_proto(proto_filename)


def get_cpp_for_type(
    name, property_obj, indent_level, val_level, main_object_available
):
    body = ""

    indent1 = "    " * indent_level
    indent2 = indent1 + "    "
    indent3 = indent2 + "    "
    indent4 = indent3 + "    "
    indent5 = indent4 + "    "
    indent6 = indent5 + "    "
    indent7 = indent6 + "    "

    if isinstance(property_obj, TypeDef):
        body += get_cpp_for_type(
            name, property_obj.basetype, indent_level, val_level, False
        )

    if property_obj == BaseType.STRING or property_obj == BaseType.GUID:
        body += (
            indent4
            + "const std::string* val_str = value{}.get_ptr<std::string*>();\n".format(
                val_level
            )
        )
        body += indent4 + "if (val_str)\n"
        body += indent4 + "{\n"
        if main_object_available:
            body += indent5 + "*responsevalue{} = *val_str;\n".format(val_level)
        else:
            body += indent5 + "responsevalue{}->set_{}(*val_str);\n".format(
                val_level, name.lower()
            )
        body += indent4 + "}\n"
    if property_obj == BaseType.BOOLEAN:
        body += indent4 + "const bool* val_bool = value{}.get_ptr<bool*>();\n".format(
            val_level
        )
        body += indent4 + "if (val_bool)\n"
        body += indent4 + "{\n"
        if main_object_available:
            body += indent5 + "*responsevalue{} = *val_bool;\n".format(val_level)
        else:
            body += indent5 + "responsevalue{}->set_{}(*val_bool);\n".format(
                val_level, name.lower()
            )
        body += indent4 + "}\n"
    if property_obj == BaseType.DECIMAL:
        body += (
            indent4
            + "const double* val_double = value{}.get_ptr<double*>();\n".format(
                val_level
            )
        )
        body += indent4 + "if (val_double)\n"
        body += indent4 + "{\n"
        if main_object_available:
            body += indent5 + "*responsevalue{} = *val_double;\n".format(val_level)
        else:
            body += indent5 + "responsevalue{}->set_{}(*val_double);\n".format(
                val_level, name.lower()
            )
        body += indent4 + "}\n"
    if property_obj == BaseType.INT64 or property_obj == BaseType.INT32:
        body += (
            indent4
            + "const int64_t* val_int64 = value{}.get_ptr<int64_t*>();\n".format(
                val_level
            )
        )
        body += indent4 + "if (val_int64)\n"
        body += indent4 + "{\n"
        if main_object_available:
            body += indent5 + "*responsevalue{} = *val_int64;\n".format(val_level)
        else:
            body += indent5 + "responsevalue{}->set_{}(*val_int64);\n".format(
                val_level, name.lower()
            )
        body += indent4 + "}\n"
    if property_obj == BaseType.TIME or property_obj == BaseType.DURATION:
        # todo(ed)
        pass
    if isinstance(property_obj, NavigationProperty):
        body += get_cpp_for_type("", property_obj.type, indent_level, val_level, False)
    if isinstance(property_obj, Collection):
        body += indent1 + "if (value{}.is_array())\n".format(val_level)
        body += indent1 + "{\n"
        val_level += 1
        body += (
            indent2
            + "for (const auto& [index{0}, value{0}] : value{1}.items())\n".format(
                val_level, val_level - 1
            )
        )
        body += indent2 + "{\n"
        body += (
            indent3
            + "auto* responsevalue{} = responsevalue{}->mutable_{}()->Add();\n".format(
                val_level, val_level - 1, name.lower()
            )
        )

        body += get_cpp_for_type(
            name, property_obj.contained_type, indent_level + 3, val_level, True
        )
        body += indent2 + "}\n"
        body += indent1 + "}\n"
    if isinstance(property_obj, EntityType):
        if main_object_available:
            body += indent1 + "auto* responsevalue{} = responsevalue{};\n".format(
                val_level + 1, val_level
            )
        else:
            body += (
                indent1
                + "auto* responsevalue{} = responsevalue{}->mutable_{}();\n".format(
                    val_level + 1, val_level, name.lower()
                )
            )
        body += indent1 + "if (value{}.is_object())\n".format(val_level)
        body += indent1 + "{\n"
        val_level += 1

        body += (
            indent2
            + "for (const auto& [key{0}, value{0}] : value{1}.items())\n".format(
                val_level, val_level - 1
            )
        )

        body += indent2 + "{\n"
        prop_index = 0
        while property_obj is not None:
            for property_node in property_obj.properties:
                body += indent3
                if prop_index != 0:
                    body += "else "

                body += 'if (key{} == "{}")\n'.format(val_level, property_node.name)
                body += indent3 + "{\n"
                if isinstance(property_node, NavigationProperty):
                    if property_node.auto_expand or property_node.contains_target:
                        body += get_cpp_for_type(
                            property_node.name,
                            property_node.type,
                            indent_level + 3,
                            val_level,
                            False,
                        )
                    # TODO(ed) Navigation references should be special cased
                    # less by passing "expanding references" to calling scopes
                    elif property_node.expand_references and isinstance(
                        property_node.type, Collection
                    ):
                        body += indent4 + "if (value{}.is_array())\n".format(val_level)
                        body += indent4 + "{\n"
                        val_level += 1
                        body += (
                            indent5
                            + "for (const auto& [index{0}, value{0}] : value{1}.items())\n".format(
                                val_level, val_level - 1
                            )
                        )
                        body += indent5 + "{\n"
                        body += (
                            indent6
                            + "auto* responsevalue{} = responsevalue{}->mutable_{}()->Add();\n".format(
                                val_level, val_level - 1, property_node.name.lower()
                            )
                        )

                        body += indent6 + "if (value{}.is_object())\n".format(val_level)
                        body += indent6 + "{\n"
                        body += indent7 + "NavigationReferenceFromJson(\n"
                        body += indent7 + "value{0}, *responsevalue{0});\n".format(
                            val_level
                        )
                        body += indent6 + "}\n"
                        body += indent5 + "}\n"
                        val_level -= 1
                        body += indent4 + "}\n"

                    elif isinstance(property_node.type, Collection):
                        body += indent4 + "if (value{}.is_array())\n".format(val_level)
                        body += indent4 + "{\n"
                        val_level += 1
                        body += (
                            indent5
                            + "for (const auto& [index{0}, value{0}] : value{1}.items())\n".format(
                                val_level, val_level - 1
                            )
                        )
                        body += indent5 + "{\n"
                        body += (
                            indent6
                            + "auto* responsevalue{} = responsevalue{}->mutable_{}()->Add();\n".format(
                                val_level, val_level - 1, property_node.name.lower()
                            )
                        )

                        body += indent6 + "if (value{}.is_object())\n".format(val_level)
                        body += indent6 + "{\n"
                        body += indent7 + "NavigationReferenceFromJson(\n"
                        body += indent7 + "value{0}, *responsevalue{0});\n".format(
                            val_level
                        )
                        body += indent6 + "}\n"
                        body += indent5 + "}\n"
                        val_level -= 1
                        body += indent4 + "}\n"

                    else:
                        body += indent4 + "if (value{}.is_object())\n".format(val_level)
                        body += indent4 + "{\n"
                        body += indent5 + "NavigationReferenceFromJson(\n"
                        body += (
                            indent6
                            + "value{0}, *responsevalue{0}->mutable_{1}());\n".format(
                                val_level, property_node.name.lower()
                            )
                        )
                        body += indent4 + "}\n"
                else:
                    body += get_cpp_for_type(
                        property_node.name,
                        property_node.type,
                        indent_level,
                        val_level,
                        False,
                    )
                body += indent3 + "}\n"
                prop_index += 1
            property_obj = property_obj.basetype
            if isinstance(property_obj, Complex):
                property_obj = None
        body += indent2 + "}\n"
        body += indent1 + "}\n"
    return body


def generate_cpp_for_entity(
    entity, path="", url_path="/redfish/v1", inherit_depth=0, collectionlist=[]
):
    body = ""

    # TODO(ed) What do collections look like?
    if isinstance(entity, Collection):
        collectionlist = list(collectionlist)
        collectionlist.append(path)
        return generate_cpp_for_entity(
            entity.contained_type, path, url_path, 0, collectionlist
        )

    if isinstance(entity, TypeDef):
        # Note, because typedefs aren't a real "property" they don't increase
        # inherit_depth
        return generate_cpp_for_entity(
            entity.basetype, path, url_path, 0, collectionlist
        )

    if isinstance(entity, EntityType):
        if path == "":
            path = "ServiceRoot"

        # only generate service definition for root level objects
        if inherit_depth == 0:
            body += "    grpc::Status Get_{}(\n".format(path)
            body += "        grpc::ServerContext* context ,\n"
            body += "        const redfish_v1::Get_{}_FilterSpec* request,\n".format(
                path
            )
            body += "        {0}::{1}* responsevalue0) override\n".format(
                entity.namespace.split(".")[0], entity.name
            )

            body += "    {\n"
            # TODO(ed) this is a bad approximation, and wont work for
            # multi-level collections like ethernet
            if len(collectionlist) != 0:
                newname = path.split("_")[-1].lower()
                body += (
                    "        const std::string& uri = request->{}id().id();\n".format(
                        newname
                    )
                )
                body += "        nlohmann::json value0 = request_uri(uri);\n"
            else:
                body += '        nlohmann::json value0 = request_uri("{}");\n'.format(
                    url_path
                )
            body += "\n"
            body += get_cpp_for_type("sroot", entity, 2, 0, True)
            body += "        return grpc::Status::OK;\n"
            body += "    }\n\n"

        if entity.basetype != None:
            this_body = generate_cpp_for_entity(
                entity.basetype, path, url_path, inherit_depth + 1, collectionlist
            )
            body += this_body

        for property_obj in entity.properties:
            if not isinstance(property_obj, NavigationProperty):
                continue
            new_path = path + "_" + property_obj.name
            if new_path.startswith("_"):
                new_path = new_path[1:]

            new_url = url_path + "/" + property_obj.name
            body += generate_cpp_for_entity(
                property_obj.type, new_path, new_url, 0, collectionlist
            )

    return body


def write_cpp_code(flat_list):
    service_root = [x for x in flat_list if x.name == "ServiceRoot"]
    if len(service_root) != 1:
        raise Exception("Unable to find unique service root")
    cpp_filename = os.path.join(SCRIPT_DIR, "..", "include", "grpc_defs.hpp")
    with open(cpp_filename, "w") as cpp_file:
        body = generate_cpp_for_entity(service_root[0])
        cpp_file.write(body)


# TODO(ed) this shouldn't be a global
folders_added_to_grpc = []


def write_meson_root_config():
    filepath = os.path.join(GRPC_DIR, "meson.build")
    with open(filepath, "w") as filehandle:
        filehandle.write("protobuf_generated = []\n")

        for filename in sorted(folders_added_to_grpc):
            filehandle.write("protobuf_generated += proto_gen.process( \\\n")
            filehandle.write("    '{}', \\\n".format(filename))
            filehandle.write("    preserve_path_from : meson.current_source_dir() \\\n")
            filehandle.write(")\n")
            # filehandle.write("subdir('{}')\n".format(filename))

        filehandle.write("protobuf_generated += grpc_gen.process(\n")
        filehandle.write("'entry.proto',\n")
        filehandle.write("preserve_path_from : meson.current_source_dir()")
        filehandle.write(")\n")


def write_meson_file_for_proto(inputpath):

    dirname = os.path.dirname(inputpath)
    basename = os.path.basename(inputpath)
    meson_filename = os.path.join(dirname, "meson.build")
    dirpath = os.path.relpath(inputpath, GRPC_DIR)

    if dirpath != ".":
        folders_added_to_grpc.append(dirpath)


def get_lowest_type(this_class, depth=0):
    if not isinstance(this_class, EntityType):
        return this_class
    if this_class.abstract:
        return this_class
    if this_class.basetype is None:
        return this_class
    return get_lowest_type(this_class.basetype, depth + 1)


def find_type_for_abstract(class_list, abs):
    for element in class_list:
        lt = get_lowest_type(element)
        if lt.name == abs.name and lt.from_file == abs.from_file:
            return element
    """
    for element in class_list:
        lt = get_lowest_type(element)
        if lt.name == abs.name:
            return element
    """
    return abs


def instantiate_abstract_classes(class_list, this_class):
    if isinstance(this_class, EntityType):
        for class_to_fix in [this_class] + this_class.basetype_flat:
            if isinstance(class_to_fix, EntityType):
                for property_instance in class_to_fix.properties:
                    if isinstance(property_instance, NavigationProperty):
                        property_instance.type = find_type_for_abstract(
                            class_list, property_instance.type
                        )
                    if isinstance(property_instance, Property):
                        property_instance.type = find_type_for_abstract(
                            class_list, property_instance.type
                        )
        for class_to_fix in [this_class] + this_class.basetype_flat:
            if isinstance(class_to_fix, EntityType):
                for property_instance in class_to_fix.properties:
                    instantiate_abstract_classes(class_list, property_instance.type)

            if isinstance(class_to_fix, Collection):
                class_to_fix.contained_type = find_type_for_abstract(
                    class_list, class_to_fix.contained_type
                )
    if isinstance(this_class, Collection):
        this_class.contained_type = find_type_for_abstract(
            class_list, this_class.contained_type
        )


def remove_old_schemas(flat_list):
    # remove all but the last schema version for a type by loading them into a
    # dict with Namespace + name
    elements = {}
    for item in flat_list:
        elements[item.namespace.split(".")[0] + item.name] = item
    flat_list = [elements[item] for item in elements]
    return flat_list


def main():
    gen_protos = True
    gen_cpp = True
    if gen_protos:
        flat_list = []

        print("Reading from {}".format(REDFISH_SCHEMA_DIR))
        for root, dirs, files in os.walk(REDFISH_SCHEMA_DIR):
            # Todo(ed) Oem account service is totally wrong odata wise, and
            # its type naming conflicts with the "real" account service
            filepaths = [
                os.path.join(root, filename)
                for filename in files
                if not filename.startswith("OemAccountService")
            ]
            if multithread:
                with Pool(int(cpu_count() / 2)) as p:
                    out = p.map(parse_toplevel, filepaths)

                    flat_list.extend([item for sublist in out for item in sublist])
            else:

                for filepath in filepaths:
                    out = parse_toplevel(filepath)
                    flat_list.extend(out)

        flat_list = remove_old_schemas(flat_list)

        flat_list.sort(key=lambda x: x.name.lower())
        flat_list.sort(key=lambda x: not x.name.startswith("ServiceRoot"))
        for element in flat_list:
            instantiate_abstract_classes(flat_list, element)

        clear_and_make_output_dirs()

        for thistype in flat_list:
            # print("{}.{}".format(thistype.name, thistype.namespace))
            generate_grpc_for_type(thistype)

        write_fixed_messages()

        write_service_root(flat_list)
        write_cpp_code(flat_list)
        write_meson_root_config()

    if gen_cpp:
        for root, dirs, files in os.walk(GRPC_DIR):
            files.sort()
            for filepath in files:
                if not filepath.endswith(".proto"):
                    continue
                args = [
                    "protoc",
                    "--cpp_out",
                    "proto_out",
                    "-I",
                    GRPC_DIR,
                    os.path.join(root, filepath),
                ]
                print(" ".join(args))
                subprocess.check_output(args, cwd=GRPC_DIR)


if __name__ == "__main__":
    main()

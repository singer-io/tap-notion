from tap_notion.streams.pages import Pages
from tap_notion.streams.databases import Databases
from tap_notion.streams.users import Users
from tap_notion.streams.blocks import Blocks

STREAMS = {
    "pages": Pages,
    "databases": Databases,
    "users": Users,
    "blocks": Blocks,
}

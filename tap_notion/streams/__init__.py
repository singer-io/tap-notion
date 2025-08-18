from tap_notion.streams.pages import Pages
from tap_notion.streams.databases import Databases
from tap_notion.streams.users import Users
from tap_notion.streams.blocks import Blocks
from tap_notion.streams.comments import Comments
from tap_notion.streams.bot_user import BotUser
from tap_notion.streams.file_upload import FileUpload
from tap_notion.streams.block_children import BlockChildren
from tap_notion.streams.page_property import PageProperty

STREAMS = {
    "pages": Pages,
    "databases": Databases,
    "users": Users,
    "blocks": Blocks,
    "comments": Comments,
    "bot_user": BotUser,
    "file_upload": FileUpload,
    "block_children": BlockChildren,
    "page_property": PageProperty,

}

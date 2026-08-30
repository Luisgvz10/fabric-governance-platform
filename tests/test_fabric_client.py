import requests
import pytest
from unittest.mock import patch, MagicMock

from fabricgov.fabric_client import FabricClient


@pytest.fixture
def mock_msal():
    with patch("fabricgov.fabric_client.msal.ConfidentialClientApplication") as mock_app_cls:
        mock_app_cls.return_value.acquire_token_for_client.return_value = {"access_token": "fake-token"}
        yield mock_app_cls


@pytest.fixture
def client(mock_msal):
    return FabricClient(tenant_id="tid", client_id="cid", client_secret="secret")


# Sin access_token, el cliente no debe crearse en silencio
def test_sin_token_falla_al_crear_el_cliente(mock_msal):
    mock_msal.return_value.acquire_token_for_client.return_value = {"error": "invalid_client"}
    with pytest.raises(Exception):
        FabricClient(tenant_id="tid", client_id="cid", client_secret="secret")


# get() tiene que llamar a base_url + endpoint, nada más raro
@patch("fabricgov.fabric_client.requests.get")
def test_get_junta_base_url_con_endpoint(mock_get, client):
    mock_get.return_value.json.return_value = {}
    client.get("/workspaces")
    url_llamada = mock_get.call_args.args[0]
    assert url_llamada == "https://api.fabric.microsoft.com/v1/workspaces"


# Un error HTTP no se traga, se propaga -- mismo criterio que en 05_bronze_item_access
@patch("fabricgov.fabric_client.requests.get")
def test_get_propaga_el_error_si_la_api_falla(mock_get, client):
    mock_get.return_value.raise_for_status.side_effect = requests.exceptions.HTTPError()
    with pytest.raises(requests.exceptions.HTTPError):
        client.get("/workspaces")


# Sin continuationToken, una sola pagina y punto
@patch("fabricgov.fabric_client.requests.get")
def test_paginacion_para_si_no_hay_continuation_token(mock_get, client):
    mock_get.return_value.json.return_value = {"workspaces": [{"id": "1"}]}
    resultado = client.get_paginated("/admin/workspaces", items_key="workspaces")
    assert resultado == [{"id": "1"}]
    assert mock_get.call_count == 1


# Con continuationToken, sigue pidiendo paginas hasta que se acaba
@patch("fabricgov.fabric_client.requests.get")
def test_paginacion_sigue_mientras_haya_continuation_token(mock_get, client):
    pagina1 = MagicMock()
    pagina1.json.return_value = {"workspaces": [{"id": "1"}], "continuationToken": "tok"}
    pagina2 = MagicMock()
    pagina2.json.return_value = {"workspaces": [{"id": "2"}]}
    mock_get.side_effect = [pagina1, pagina2]

    resultado = client.get_paginated("/admin/workspaces", items_key="workspaces")

    assert resultado == [{"id": "1"}, {"id": "2"}]
    assert mock_get.call_count == 2


# folderId solo va en el cuerpo si se lo pasas
@patch("fabricgov.fabric_client.requests.post")
def test_crear_item_solo_manda_folder_id_si_se_indica(mock_post, client):
    mock_post.return_value.json.return_value = {}
    client.create_lakehouse("ws1", "lh_test")
    cuerpo_enviado = mock_post.call_args.kwargs["json"]
    assert "folderId" not in cuerpo_enviado


# El rol de admin solo se asigna si le das un principal
@patch("fabricgov.fabric_client.requests.post")
def test_crear_workspace_solo_asigna_admin_si_se_indica(mock_post, client):
    mock_post.return_value.json.return_value = {"id": "ws1"}
    client.create_workspace("ws-test")
    assert mock_post.call_count == 1  # solo la creacion, sin roleAssignments

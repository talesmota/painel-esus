import { CSSProperties, useState, useEffect } from 'react';
import Select, { StylesConfig } from 'react-select';
import { useNavigate, useParams } from 'react-router-dom';
import { useQuery } from "react-query";
import { Spinner } from 'reactstrap';

import { Header } from '../components/Header';
import { Footer } from '../components/Footer';
import { getUserLocalStorage } from '../context/AuthProvider/util';

import { formataNumero, getNomeUbs, somaIndicador } from '../utils';

import masculino from '../assets/images/masculino.svg';
import feminino from '../assets/images/feminino.svg';

import homem from '../assets/images/homem.svg';
import mulher from '../assets/images/mulher.svg';

import diabetes from '../assets/images/diabetes.svg';
import hipertensao from '../assets/images/hipertensao.svg';
import tosse from '../assets/images/tosse.svg';

import { Condicao } from "../charts/Condicao";
import Piramide from "../charts/Piramide";
import { Zonas } from "../charts/Zonas";

import '../styles/painel.scss';
import { Api } from '../services/api';
import { Api as Api2 } from "../services/api2";
import { isTemplateExpression } from 'typescript';

type PainelParams = {
    id: string;
}
type Indicator = {
    rural: number;
    urbano: number;
}

type TypeUbs = {
    label: string;
    value: number | string;
};
type TypeCondiction = {
    value: number;
    name: string;
};
const customControlStyles: CSSProperties = {
    backgroundColor: '#2775b0',
    width: "320px",
    height: "40px",
    color: "#000000"
};

type IsMulti = false;

const selectStyle: StylesConfig<TypeUbs, IsMulti> = {
    control: (provided, state) => {
        return {
            ...provided,
            ...customControlStyles
        };
    },
    option: (provided, state) => ({
        ...provided,
        color: state.isSelected ? '#FFFFFF' : '',
        backgroundColor: state.isSelected ? '#343131' : '',
        padding: 10,
    }),
    clearIndicator: () => ({
        color: '#343131',
    }),
    dropdownIndicator: () => ({
        color: '#343131',
    }),
};

interface IPainel {
    ibgePopulation: number;
    ageGroups: {},
    gender: {
        feminino: number;
        masculino: number;
    }
    indicators: {
        diabetes: Indicator;
        gestantes: Indicator;
        hipertensao: Indicator;
    },
    locationArea: {
        rural: number;
        urbano: number;
    },
    total: number;
}

type ResponseData = {
    data: IPainel;
}

type Lista = {
    co_dim_unidade_saude_1: number;
    no_unidade_saude: string;
    nu_cnes: number;
}

type ResponseDataListUbs = {
    data: Lista[];
}
type TResponse = {
    co_dim_tempo: string;
    nu_cnes: string;
    type: string;
    ds_filtro_cids: number;
    local: string;
}

export function Painel() {
    let navigate = useNavigate();
    const user = getUserLocalStorage();
    const { id } = useParams<PainelParams>();

    const [dadosPainel, setDadosPainel] = useState<IPainel>();
    const [loading, setLoading] = useState(true);
    const [infecoesQtd, setInfecoesQtd] = useState<TypeCondiction[]>([
        {value: 12, name: 'Rural'},{value: 21, name: 'Urbano'},
    ]);
    //get-demographic-info
    useEffect(() => {
        const getDados = async () => {
            let rota = id ? `get-demographic-info/${id}` : 'get-demographic-info';

            try {
                const response = await Api.get<ResponseData>(rota);
                const { data } = response.data;

                setDadosPainel(data);
                setLoading(false);
            } catch (error) {
                alert("Ocorreu um erro ao buscar informações demográficas");
                setLoading(false);
            }
        }

        getDados();

        return () => {
            console.log('...');
        }
    }, [id]);

    const { data: sindromesAgudasData, isLoading, error } = useQuery(['sindromes-agudas', id], async () => {
        // let path = id ? `pregnants/exams-table/${id}` : 'pregnants/exams-table';
        let path = '/saida.json';
        const response = await Api2.get(path);
        let data = response.data;
        const result = [
            {value: 0, name: 'Rural'},
            {value: 0, name: 'Urbano'},
        ]
        if (id) {
            data = data.filter( (item:TResponse) => item.nu_cnes == id)
        }
        let total = 0;
        data.map( (item:TResponse) => {
            
            if (item.local == 'Rural') {
                result[0].value+= 1;
            } else {
                result[1].value+=1;
            }
            total+=1
        })
        setInfecoesQtd(result)
        return data;
    });
    //get nome ubs
    const { data: dataUbs, isLoading: isLoadingUbs, error: errorUbs } = useQuery('ubs', async () => {
        const response = await Api.get<ResponseDataListUbs>('get-units')
        const data = response.data
        const listData: TypeUbs[] = data.data.map((ubs) => {
            return {
                "label": ubs.no_unidade_saude,
                "value": ubs.nu_cnes,
            }
        })

        return listData;
    }, {
        staleTime: 1000 * 60 * 10, //10 minutos
    });

    //get nome ubs
    const nomeUbs = id && !isLoadingUbs ? getNomeUbs(dataUbs, id) : '-';

    function handleToPainelMunicipio() {
        setLoading(true);
        navigate('/painelx');
    }

    const onChangeSelection = (e: any) => {
        setLoading(true);
        navigate(`/painel/${e.value}`);
    };

    function handleToGestante() {
        if (id !== undefined) {
            navigate(`/gestantes/${id}`);
        } else {
            navigate('/gestantes');
        }
    }

    function handleToDiabetes() {
        if (id !== undefined) {
            navigate(`/diabetes/${id}`);
        } else {
            navigate('/diabetes');
        }
    }

    function handleToHipertensao() {
        if (id !== undefined) {
            navigate(`/hipertensao/${id}`);
        } else {
            navigate('/hipertensao');
        }
    }
    function handleToSindromesAgudas() {
        if (id !== undefined) {
            navigate(`/sindromes-agudas/${id}`);
        } else {
            navigate('/sindromes-agudas');
        }
    }
    return (
        <div id="page-painel">
            <Header />

            {loading ? (
                <div className="contentWrapperLoading">
                    <Spinner color="#343131" />
                </div>
            ) : (
                <div className="contentWrapper">

                    <hr className="linha my-4" />

                    <h2>
                        {id ? (!isLoadingUbs ? nomeUbs : 'Carregando...') : user.municipio + " - " + user.uf}
                    </h2>

                    <div className="container container-cards-principal">
                        <div className="row align-items-start">
                            <div className="col-xl-4">
                                <div className="container-card d-flex flex-column flex-md-row align-items-center justify-content-center my-2 py-2 px-4">
                                    <div className="w-50 d-flex flex-column align-items-center justify-content-center">
                                        <h4 className="text-center">Cidadãos Cadastrados</h4>
                                        <span>{formataNumero(dadosPainel?.total)}</span>
                                    </div>
                                    <div className="w-50 d-flex flex-column align-items-center justify-content-center">
                                        <h4 className="text-center">População Estimada</h4>
                                        <span>{formataNumero(dadosPainel?.ibgePopulation)}</span>
                                    </div>
                                </div>
                            </div>

                            <div className="col-xl-4">
                                <div className="container-card-alt d-flex flex-column flex-md-row align-items-center justify-content-center my-2">
                                    <div className="me-2">
                                        <Zonas data={dadosPainel?.locationArea} />
                                    </div>

                                    <div className="container-dados-zona">
                                        <div className="d-flex align-items-center mb-2">
                                            <div className="box-container-light me-2"></div>
                                            <h4>Zona Urbana</h4>
                                        </div>
                                        <span>{formataNumero(dadosPainel?.locationArea.urbano)}</span>
                                    </div>

                                    <div className="container-dados-zona">
                                        <div className="d-flex align-items-center mb-2">
                                            <div className="box-container-dark me-2"></div>
                                            <h4>Zona Rural</h4>
                                        </div>
                                        <span>{formataNumero(dadosPainel?.locationArea.rural)}</span>
                                    </div>
                                </div>
                            </div>

                            <div className="col-xl-4">
                                <div className="container-card d-flex align-items-center justify-content-center my-2 py-1">
                                    <div className="d-flex flex-column align-items-center ms-2 me-4">
                                        <img className="my-2" src={homem} alt="Homem" width={60} />
                                        <span>
                                            {formataNumero(dadosPainel?.gender.masculino)}
                                        </span>
                                    </div>

                                    <div className="d-flex flex-column align-items-center ms-4 me-2">
                                        <img className="my-2" src={mulher} alt="Mulher" width={60} />
                                        <span>
                                            {formataNumero(dadosPainel?.gender.feminino)}
                                        </span>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>

                    <h3 className="my-5">Proporção de indivíduos cadastrados segundo sexo e idade</h3>

                    {(dadosPainel !== undefined && dadosPainel.ageGroups && Object.keys(dadosPainel.ageGroups).length !== 4) ? (
                        <div className='graficoPiramide'>
                            <div className='w-100 painel-demografico'>
                                <div className='d-flex justify-content-center'>
                                    <div className='mx-2'>
                                        <img src={masculino} className="img-fluid" alt="Masculino" />
                                    </div>
                                    <div className='mx-2'>
                                        <img src={feminino} className="img-fluid" alt="Feminino" />
                                    </div>
                                </div>

                                <Piramide data={dadosPainel.ageGroups} />
                            </div>

                            <div className='d-flex align-items-center justify-content-between mt-5'>
                                <div className="d-flex align-items-center mx-3">
                                    <div className="box-container-light me-2"></div>
                                    <h5 className='mb-0'>Área Urbana</h5>
                                </div>

                                <div className="d-flex align-items-center mx-3">
                                    <div className="box-container-dark me-2"></div>
                                    <h5 className='mb-0'>Área Rural</h5>
                                </div>
                            </div>

                        </div>
                    ) : (
                        <h6 className='text-danger'>Sem dados de proporção de indivíduos cadastrados.</h6>
                    )}

                    <h3 className="my-5">Condição de saúde dos indivíduos cadastrados no município</h3>

                    <div className="container">
                        <div className="row container-cards-condicoes">
                            <div className="card-condicao p-2" onClick={handleToDiabetes}>
                                <span className="nome-condicao">Diabetes</span>
                                <h4>{somaIndicador(dadosPainel?.indicators.diabetes)}</h4>

                                <div className="d-flex align-items-center">
                                    <img src={diabetes} alt="Diabetes" className="mx-2" />
                                    <Condicao data={dadosPainel?.indicators.diabetes} />
                                </div>
                            </div>

                            <div className="card-condicao p-2" onClick={handleToHipertensao}>
                                <span className="nome-condicao">Hipertensão</span>

                                <h4>{somaIndicador(dadosPainel?.indicators.hipertensao)}</h4>

                                <div className="d-flex align-items-center">
                                    <img src={hipertensao} alt="Hipertensão" className="mx-2" />
                                    <Condicao data={dadosPainel?.indicators.hipertensao} />
                                </div>
                            </div>

                            {/* <div className="card-condicao p-2" onClick={handleToGestante}>
                                <span className="nome-condicao">Gestantes</span>
                                <h4>{somaIndicador(dadosPainel?.indicators.gestantes)}</h4>

                                <div className="d-flex align-items-center">
                                    <img src={gestantes} alt="Gestantes" className="mx-2" />
                                    <Condicao data={dadosPainel?.indicators.gestantes} />
                                </div>
                            </div> */}
                            <div className="card-condicao p-2" onClick={handleToSindromesAgudas}>
                                <span className="nome-condicao">Síndromes Agudas</span>
                                <h4>{somaIndicador({
                                        rural: infecoesQtd[0].value,
                                        urbano: infecoesQtd[1].value,
                                    })}</h4>

                                <div className="d-flex align-items-center">
                                    <img src={tosse} alt="Sindrome Aguda" className="mx-2" />
                                    <Condicao data={{
                                        rural: infecoesQtd[0].value,
                                        urbano: infecoesQtd[1].value,
                                    }} />
                                </div>
                            </div>
                        </div>

                        <div className="d-flex my-5 justify-content-center">
                            <div className="container-areas d-flex align-items-center me-4">
                                <div className="box-container-light me-2"></div>
                                <h4>Área Urbana</h4>
                            </div>

                            <div className="container-areas d-flex align-items-center ms-4">
                                <div className="box-container-dark me-2"></div>
                                <h4>Área Rural</h4>
                            </div>
                        </div>
                    </div>

                    <div className='my-5'>
                        {id ? (
                            <button
                                type="button"
                                onClick={handleToPainelMunicipio}
                                className="btn btn-primary">
                                Visualizar dados do painel do Município
                            </button>
                        ) : (
                            <>
                                {isLoadingUbs ?
                                    (
                                        <div className="combo-ubs d-flex align-items-center justify-content-center"><Spinner size="sm me-1" /> Carregando lista de UBS's</div>
                                    ) : errorUbs ? (
                                        <div className="combo-ubs d-flex align-items-center justify-content-center">Falha ao carregar lista de UBS's</div>
                                    ) : (
                                        <Select
                                            isClearable
                                            placeholder="Selecione UBS"
                                            noOptionsMessage={() => "Nenhuma UBS encontrada"}
                                            options={dataUbs}
                                            styles={selectStyle}
                                            onChange={onChangeSelection}
                                        />
                                    )}
                            </>
                        )}
                    </div>
                </div>
            )}

            <Footer />
        </div>
    )
}